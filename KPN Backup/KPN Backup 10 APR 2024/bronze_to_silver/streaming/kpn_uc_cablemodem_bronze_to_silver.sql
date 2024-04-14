-- Databricks notebook source
-- MAGIC %md
-- MAGIC # IMPORTING LIBRARIES AND INITIALIZING VARIABLES

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import datetime,requests,json,sys
-- MAGIC sys.path.append('/Workspace/Shared/KPN-TEST/utils/')
-- MAGIC from config import *
-- MAGIC from urllib.parse import urljoin
-- MAGIC import pandas as pd
-- MAGIC from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
-- MAGIC from DataQualityAndJobAuditManager import DataQualityManager
-- MAGIC from DataQualityAndJobAuditManager import AuditLogManager
-- MAGIC
-- MAGIC # Variables
-- MAGIC topic=CABLEMODEM
-- MAGIC CATALOG=NETWORK
-- MAGIC user_name=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
-- MAGIC notebook_path=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
-- MAGIC job_id = dbutils.widgets.get('job_id')
-- MAGIC run_id = dbutils.widgets.get('job.run_id')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Initialize AuditLogManager instance
-- MAGIC audit_log_manager = AuditLogManager()
-- MAGIC
-- MAGIC # Initialize DataQualityManager instance
-- MAGIC data_quality_manager=DataQualityManager()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #DATA LOADING AND TABLES CREATION

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Set the timeParserPolicy to LEGACY mode for Spark SQL
-- MAGIC spark.sql(f"""SET spark.sql.legacy.timeParserPolicy = LEGACY;""")
-- MAGIC
-- MAGIC # Flag records from Bronze table, that will be used for Curation
-- MAGIC spark.sql(f"""
-- MAGIC             UPDATE kpn_bronze.network.cablemodem_dt
-- MAGIC             SET wm_flag = "NT"
-- MAGIC             WHERE wm_flag="N";
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC # exit notebook if there is not data in source table
-- MAGIC audit_log_manager.handle_source_table_empty(spark,topic,DATABRICKS_HOST,DATABRICKS_TOKEN,DBR_JOBS_API_ENDPOINT,CATALOG)
-- MAGIC
-- MAGIC # extract dataquality conditions
-- MAGIC good_condition=data_quality_manager.dq_from_information_schema_uc_bronze(spark,topic)
-- MAGIC
-- MAGIC # Creating a temp table with the good_bad_flag
-- MAGIC spark.sql(f"""
-- MAGIC             create or replace table kpn_silver.default.{topic}_dt_temp
-- MAGIC             select *,case when {good_condition} then 1 else 0 end as good_bad_flag
-- MAGIC             from kpn_bronze.network.{topic}_dt
-- MAGIC             WHERE wm_flag='NT' 
-- MAGIC """)
-- MAGIC
-- MAGIC # Insert into Curated Table, with DQ Rules enforces
-- MAGIC df_good=spark.sql(f"""
-- MAGIC INSERT INTO kpn_silver.network.{topic}_dt(
-- MAGIC   HUB,
-- MAGIC   MAC_ADDR,
-- MAGIC   METRICS_DATE,
-- MAGIC   DOWNSTREAM_NAME,
-- MAGIC   CER_DN,
-- MAGIC   CCER_DN,
-- MAGIC   SNR_DN,
-- MAGIC   RX_POWER_DN,
-- MAGIC   CRTD_DTTM ,
-- MAGIC   CRTD_BY
-- MAGIC )
-- MAGIC SELECT 
-- MAGIC   HUB,
-- MAGIC   MAC_ADDR,
-- MAGIC   to_timestamp(METRICS_DATE,'yyyy-MM-dd HH:mm:ss') AS METRICS_DATE,
-- MAGIC   DOWNSTREAM_NAME,
-- MAGIC   CAST(CER_DN AS BIGINT),
-- MAGIC   CAST(CCER_DN AS BIGINT),
-- MAGIC   SNR_DN,
-- MAGIC   RX_POWER_DN,
-- MAGIC   current_timestamp() AS CRTD_DTTM,
-- MAGIC   'data_engineer_group' AS CRTD_BY
-- MAGIC FROM kpn_silver.default.{topic}_dt_temp
-- MAGIC WHERE good_bad_flag=1 ;""")
-- MAGIC
-- MAGIC # Insert into Curated Table, with DQ Rules enforces
-- MAGIC
-- MAGIC df_bad=spark.sql(f"""
-- MAGIC INSERT INTO kpn_silver.network.rejection_{topic}_dt (
-- MAGIC   HUB,
-- MAGIC   MAC_ADDR,
-- MAGIC   METRICS_DATE,
-- MAGIC   DOWNSTREAM_NAME,
-- MAGIC   CER_DN,
-- MAGIC   CCER_DN,
-- MAGIC   SNR_DN,
-- MAGIC   RX_POWER_DN,
-- MAGIC   DQ_STATUS,
-- MAGIC   CRTD_DTTM,
-- MAGIC   CRTD_BY
-- MAGIC )
-- MAGIC SELECT 
-- MAGIC   HUB,
-- MAGIC   MAC_ADDR,
-- MAGIC   to_timestamp(METRICS_DATE,'yyyy-MM-dd HH:mm:ss') AS METRICS_DATE,
-- MAGIC   DOWNSTREAM_NAME,
-- MAGIC   CAST(CER_DN AS BIGINT),
-- MAGIC   CAST(CCER_DN AS BIGINT),
-- MAGIC   SNR_DN,
-- MAGIC   RX_POWER_DN,
-- MAGIC   'O' as DQ_STATUS,
-- MAGIC   current_timestamp() AS CRTD_DTTM ,
-- MAGIC   'data_engineer_group' AS CRTD_BY
-- MAGIC    FROM kpn_silver.default.{topic}_dt_temp
-- MAGIC WHERE WHERE good_bad_flag=0
-- MAGIC """)
-- MAGIC
-- MAGIC # saved number edited records for auditing
-- MAGIC rows_affected_good_table=df_good.first().num_affected_rows
-- MAGIC rows_affected_bad_table=df_bad.first().num_affected_rows
-- MAGIC spark.sql(f"""drop table kpn_silver.default.{topic}_dt_temp;""")
-- MAGIC
-- MAGIC # Updating curated records back in the source table to mark them as processed
-- MAGIC spark.sql(f"""UPDATE kpn_bronze.network.{topic}_dt
-- MAGIC SET wm_flag = "Y" -- Setting wm_flag to 'Y' indicates these records have been processed
-- MAGIC WHERE wm_flag="NT"; -- Selecting records with wm_flag 'NT' to update
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AUDIT

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Call methods to create and save audit tables
-- MAGIC audit_row_df = audit_log_manager.create_audit_table_row(spark, topic, job_id, run_id, rows_affected_good_table, rows_affected_bad_table, user_name, notebook_path)
-- MAGIC audit_row_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_tables")
-- MAGIC
-- MAGIC job_info_df = audit_log_manager.fetch_job_info_api(spark,DATABRICKS_HOST, DATABRICKS_TOKEN,DBR_JOBS_API_ENDPOINT)
-- MAGIC job_info_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_jobs")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #DATA QUALITY
-- MAGIC

-- COMMAND ----------

-- STEP 2: Data Rectification Logic
-- 1. Identify rectified records from the source table based on composite
-- 2. Insert records good table
-- 3. update timestamp and DQ_STATUS = "R"

-- -- 1. Rectified Records --
-- CREATE OR REPLACE TEMP VIEW cablemodem_rectified_records
-- AS 
-- SELECT source_cablemodem.* FROM kpn_bronze.network.cablemodem_dt source_cablemodem
-- INNER JOIN kpn_silver.network.rejection_cablemodem_dt target_cablemodem
-- ON source_cablemodem.HUB=target_cablemodem.HUB
-- AND source_cablemodem.RX_POWER_DN=target_cablemodem.RX_POWER_DN
-- WHERE source_cablemodem.WM_FLAG="NT"
-- AND source_cablemodem.METRICS_DATE RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$')
-- AND source_cablemodem.MAC_ADDR != ''

-- -- 2. Insert Rectified recrods in the good table --
-- INSERT INTO kpn_silver.network.cablemodem_dt(
--   CCER_DN,
--   CER_DN, 
--   DOWNSTREAM_NAME, 
--   HUB, 
--   MAC_ADDR, 
--   METRICS_DATE, 
--   RX_POWER_DN, 
--   SNR_DN,
--   CRTD_DTTM, 
--   CRTD_BY)
-- SELECT 
--   CCER_DN,
--   CER_DN, 
--   DOWNSTREAM_NAME, 
--   HUB, 
--   MAC_ADDR, 
--   METRICS_DATE, 
--   RX_POWER_DN, 
--   SNR_DN,
--   current_timestamp() AS CRTD_DTTM,
--   'data_engineer_group' AS CRTD_BY
-- FROM cablemodem_rectified_records

-- -- 3. Status updated to Resolved --
-- MERGER INTO kpn_silver.network.rejection_cablemodem_dt AS target_cablemodem
-- USING cablemodem_rectified_records AS source_cablemodem
-- ON target_cablemodem.HUB = source_cablemodem.HUB
-- AND target_cablemodem.RX_POWER_DN = source_cablemodem.RX_POWER_DN
-- WHEN MATCHED THEN
-- UPDATE SET target_cablemodem.DQ_STATUS = 'R',target_cablemodem.CRTD_DTTM = current_timestamp()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #INCREMENTAL LOAD

-- COMMAND ----------

-- MERGE INTO kpn_silver.network.cablemodem_dt AS t 
-- USING kpn_bronze.network.cablemodem_dt AS s
-- ON t.HUB = s.HUB 
-- AND t.RX_POWER_DN = s.RX_POWER_DN
-- WHEN NOT MATCHED AND s.METRICS_DATE RLIKE ('^[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}') AND MAC_ADDR != '' THEN
-- INSERT(
--  HUB,
--  MAC_ADDR,
--  METRICS_DATE,
--  DOWNSTREAM_NAME,
--  CER_DN,
--  CCER_DN,
--  SNR_DN,
--  RX_POWER_DN,
--  CRTD_DTTM,
--  CRTD_BY)

-- VALUES(
--  s.HUB,
--  s.MAC_ADDR,
--  to_timestamp(s.METRICS_DATE,'yyyy/MM/dd HH:mm'),
--  s.DOWNSTREAM_NAME,
--  s.CER_DN,
--  s.CCER_DN,
--  s.SNR_DN,
--  s.RX_POWER_DN,
--  current_timestamp(),
--  'data_engineer_group')