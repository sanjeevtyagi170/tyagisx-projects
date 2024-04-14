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
-- MAGIC  # Variables
-- MAGIC topic=BREAKDOWN
-- MAGIC CATALOG=CUSTOMERSERVICE
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
-- MAGIC # Step 1: Flag records from Bronze table, that will be used for Curation
-- MAGIC
-- MAGIC spark.sql(f"""
-- MAGIC             UPDATE kpn_bronze.customerservice.{topic}_dt
-- MAGIC             SET wm_flag = "NT"
-- MAGIC             WHERE wm_flag="N";
-- MAGIC """)
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
-- MAGIC             from kpn_bronze.customerservice.{topic}_dt
-- MAGIC             WHERE wm_flag='NT' 
-- MAGIC             """)
-- MAGIC
-- MAGIC # Insert Curated into Good Table, with DQ Rules enforces
-- MAGIC df_good=spark.sql(f"""
-- MAGIC             INSERT INTO kpn_silver.customerservice.{topic}_dt(
-- MAGIC             ID_BREAKDOWN,
-- MAGIC             COD_AC,
-- MAGIC             COD_PT,
-- MAGIC             TOTAL_LR,
-- MAGIC             TOTAL_BREAKDOWN,
-- MAGIC             DATE_CREATION,
-- MAGIC             DATE_CONFIRMATION,
-- MAGIC             DATE_FETCH,
-- MAGIC             COD_GR_ELEM,
-- MAGIC             PARQUE,
-- MAGIC             DATE_FORECAST,
-- MAGIC             DATE_FORECAST_END,
-- MAGIC             OBSERVATION,
-- MAGIC             REFERENCE,
-- MAGIC             REFERENCE_INDISP,
-- MAGIC             CRTD_DTTM, 
-- MAGIC             CRTD_BY
-- MAGIC             )
-- MAGIC             SELECT 
-- MAGIC             CAST(ID_BREAKDOWN AS BIGINT),
-- MAGIC             COD_AC,
-- MAGIC             COD_PT,
-- MAGIC             CAST(TOTAL_LR AS BIGINT),
-- MAGIC             CAST(TOTAL_BREAKDOWN AS BIGINT),
-- MAGIC             to_timestamp(DATE_CREATION ,'yyyy-MM-dd HH:mm:ss') AS DATE_CREATION,
-- MAGIC             to_timestamp(DATE_CONFIRMATION ,'yyyy-MM-dd HH:mm:ss') AS DATE_CONFIRMATION,
-- MAGIC             to_timestamp(DATE_FETCH ,'yyyy-MM-dd HH:mm:ss') AS DATE_FETCH,
-- MAGIC             COD_GR_ELEM,
-- MAGIC             CAST(PARQUE AS BIGINT),
-- MAGIC             to_timestamp(DATE_FORECAST ,'yyyy-MM-dd HH:mm:ss') AS DATE_FORECAST,
-- MAGIC             to_timestamp(DATE_FORECAST_END ,'yyyy-MM-dd HH:mm:ss') AS DATE_FORECAST_END,
-- MAGIC             OBSERVATION,
-- MAGIC             REFERENCE,
-- MAGIC             REFERENCE_INDISP,
-- MAGIC             current_timestamp() AS CRTD_DTTM,
-- MAGIC             'data_engineer_group' AS CRTD_BY
-- MAGIC             FROM kpn_silver.default.{topic}_dt_temp
-- MAGIC             WHERE good_bad_flag=1 ;
-- MAGIC """)
-- MAGIC
-- MAGIC # Insert into Curated Table, with DQ Rules enforces
-- MAGIC df_bad=spark.sql(f"""
-- MAGIC INSERT INTO kpn_silver.customerservice.rejection_{topic}_dt (
-- MAGIC   ID_BREAKDOWN,
-- MAGIC   COD_AC,
-- MAGIC   COD_PT,
-- MAGIC   TOTAL_LR,
-- MAGIC   TOTAL_BREAKDOWN,
-- MAGIC   DATE_CREATION,
-- MAGIC   DATE_CONFIRMATION,
-- MAGIC   DATE_FETCH,
-- MAGIC   COD_GR_ELEM,
-- MAGIC   PARQUE,
-- MAGIC   DATE_FORECAST,
-- MAGIC   DATE_FORECAST_END,
-- MAGIC   OBSERVATION,
-- MAGIC   REFERENCE,
-- MAGIC   REFERENCE_INDISP,
-- MAGIC   DQ_STATUS,
-- MAGIC   CRTD_DTTM,
-- MAGIC   CRTD_BY
-- MAGIC )
-- MAGIC SELECT 
-- MAGIC   CAST(ID_BREAKDOWN AS BIGINT),
-- MAGIC   COD_AC,
-- MAGIC   COD_PT,
-- MAGIC   CAST(TOTAL_LR AS BIGINT),
-- MAGIC   CAST(TOTAL_BREAKDOWN AS BIGINT),
-- MAGIC   to_timestamp(DATE_CREATION ,'yyyy-MM-dd HH:mm:ss') AS DATE_CREATION,
-- MAGIC   to_timestamp(DATE_CONFIRMATION ,'yyyy-MM-dd HH:mm:ss') AS DATE_CONFIRMATION,
-- MAGIC   to_timestamp(DATE_FETCH ,'yyyy-MM-dd HH:mm:ss') AS DATE_FETCH,
-- MAGIC   COD_GR_ELEM,
-- MAGIC   CAST(PARQUE AS BIGINT),
-- MAGIC   to_timestamp(DATE_FORECAST ,'yyyy-MM-dd HH:mm:ss') AS DATE_FORECAST,
-- MAGIC   to_timestamp(DATE_FORECAST_END ,'yyyy-MM-dd HH:mm:ss') AS DATE_FORECAST_END,
-- MAGIC   OBSERVATION,
-- MAGIC   REFERENCE,
-- MAGIC   REFERENCE_INDISP,
-- MAGIC   'O' as DQ_STATUS,
-- MAGIC   current_timestamp() AS CRTD_DTTM,
-- MAGIC   'data_engineer_group' AS CRTD_BY
-- MAGIC    FROM kpn_silver.default.{topic}_dt_temp
-- MAGIC WHERE WHERE good_bad_flag=0
-- MAGIC """)
-- MAGIC
-- MAGIC # Saved count to records for auditing
-- MAGIC rows_affected_good_table=df_good.first().num_affected_rows
-- MAGIC rows_affected_bad_table=df_bad.first().num_affected_rows
-- MAGIC spark.sql(f"""drop table kpn_silver.default.{topic}_dt_temp;""")
-- MAGIC
-- MAGIC
-- MAGIC #STEP 5: Updating curated records back in the source table to mark them as processed
-- MAGIC spark.sql(f"""
-- MAGIC UPDATE kpn_bronze.customerservice.{topic}_dt
-- MAGIC SET wm_flag = "Y" -- Setting wm_flag to 'Y' indicates these records have been processed
-- MAGIC WHERE wm_flag="NT"; 
-- MAGIC """) # Selecting records with wm_flag 'NT' to update
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AUDIT

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Call methods to create and save audit tables
-- MAGIC audit_row_df = audit_log_manager.create_audit_table_row(spark,topic, job_id, run_id, rows_affected_good_table, rows_affected_bad_table, user_name, notebook_path)
-- MAGIC audit_row_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_tables")
-- MAGIC
-- MAGIC job_info_df = audit_log_manager.fetch_job_info_api(spark,DATABRICKS_HOST, DATABRICKS_TOKEN,DBR_JOBS_API_ENDPOINT)
-- MAGIC job_info_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_jobs")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DATA QUALITY

-- COMMAND ----------

-- STEP 2 Data Rectification Logic
-- 1. Identify rectified records from the source table based on composite
-- 2. Insert records good table
-- 3. update timestamp and DQ_STATUS = "R"

-- -- 1. Rectified Records --
-- CREATE OR REPLACE TEMP VIEW breakdown_rectified_records
-- AS 
-- SELECT source_breakdown.* FROM kpn_bronze.customerservice.breakdown_dt source_breakdown
-- INNER JOIN kpn_silver.customerservice.rejection_breakdown_dt target_breakdown
-- ON source_breakdown.ID_BREAKDOWN = target_breakdown.ID_BREAKDOWN
-- WHERE source_breakdown.WM_FLAG = "NT"
-- AND source_breakdown.OBSERVATION RLIKE '[^\\d.-]'
-- AND source_breakdown.OBSERVATION != ''

-- -- 2. Insert Rectified records in the good table --
-- INSERT INTO kpn_silver.customerservice.breakdown_dt_silver(
--   ID_BREAKDOWN, 
--   COD_AC, 
--   COD_PT, 
--   TOTAL_LR, 
--   TOTAL_BREAKDOWN, 
--   DATE_CREATION, 
--   DATE_CONFIRMATION, 
--   DATE_FETCH, 
--   COD_GR_ELEM, 
--   PARQUE, 
--   DATE_FORECAST, 
--   DATE_FORECAST_END, 
--   OBSERVATION, 
--   REFERENCE, 
--   REFERENCE_INDISP,
--   CRTD_DTTM, 
--   CRTD_BY)
-- SELECT 
--   ID_BREAKDOWN, 
--   COD_AC, 
--   COD_PT, 
--   TOTAL_LR, 
--   TOTAL_BREAKDOWN, 
--   DATE_CREATION, 
--   DATE_CONFIRMATION, 
--   DATE_FETCH, 
--   COD_GR_ELEM, 
--   PARQUE, 
--   DATE_FORECAST, 
--   DATE_FORECAST_END, 
--   OBSERVATION, 
--   REFERENCE, 
--   REFERENCE_INDISP,
--   current_timestamp() AS CRTD_DTTM,
--   'data_engineer_group' AS CRTD_BY
-- FROM breakdown_rectified_records


-- -- 3. Status updated to Resolved --
-- MERGE INTO kpn_silver.customerservice.rejection_breakdown_dt AS target_breakdown
-- USING breakdown_rectified_records AS source_breakdown
-- ON target_breakdown.ID_BREAKDOWN = source_breakdown.ID_BREAKDOWN
-- WHEN MATCHED THEN
-- UPDATE SET target_breakdown.DQ_STATUS = 'R',target_breakdown.CRTD_DTTM=current_timestamp()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #INCREMENTAL LOAD

-- COMMAND ----------

-- MERGE INTO kpn_silver.customerservice.breakdown_dt AS t 
-- USING kpn_bronze.customerservice.breakdown_dt AS s
-- ON t.ID_BREAKDOWN = s.ID_BREAKDOWN
-- WHEN NOT MATCHED AND OBSERVATION RLIKE '[^\\d.-]' AND OBSERVATION != '' THEN
-- INSERT(
--  ID_BREAKDOWN,
--  COD_AC,
--  COD_PT,
--  TOTAL_LR,
--  TOTAL_BREAKDOWN,
--  DATE_CREATION,
--  DATE_CONFIRMATION,
--  DATE_FETCH,
--  COD_GR_ELEM,
--  PARQUE,
--  DATE_FORECAST,
--  DATE_FORECAST_END,
--  OBSERVATION,
--  REFERENCE,
--  REFERENCE_INDISP,
--  CRTD_DTTM, 
--  CRTD_BY)

-- VALUES(
--  CAST(s.ID_BREAKDOWN AS BIGINT),
--  s.COD_AC,s.COD_PT,
--  CAST(s.TOTAL_LR AS BIGINT),
--  CAST(s.TOTAL_BREAKDOWN AS BIGINT),
--  to_timestamp(s.DATE_CREATION ,'yyyy-MM-dd HH:mm:ss') AS DATE_CREATION,
--  to_timestamp(s.DATE_CONFIRMATION ,'yyyy-MM-dd HH:mm:ss') AS DATE_CONFIRMATION,
--  to_timestamp(s.DATE_FETCH ,'yyyy-MM-dd HH:mm:ss') AS DATE_FETCH,
--  s.COD_GR_ELEM,CAST(s.PARQUE AS BIGINT),
--  to_timestamp(s.DATE_FORECAST ,'yyyy-MM-dd HH:mm:ss') AS DATE_FORECAST,
--  to_timestamp(s.DATE_FORECAST_END ,'yyyy-MM-dd HH:mm:ss') AS DATE_FORECAST_END,
--  s.OBSERVATION,
--  s.REFERENCE,
--  s.REFERENCE_INDISP,
--  current_timestamp() AS CRTD_DTTM,
-- 'data_engineer_group' AS CRTD_BY)