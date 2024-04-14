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
-- MAGIC topic=INHOME
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
-- MAGIC #  Set the timeParserPolicy to LEGACY mode for Spark SQL
-- MAGIC spark.sql(f"""SET spark.sql.legacy.timeParserPolicy = LEGACY;""")
-- MAGIC
-- MAGIC #  Step 1: Flag records from Bronze table, that will be used for Curation
-- MAGIC spark.sql(f"""
-- MAGIC             UPDATE kpn_bronze.network.{topic}_dt
-- MAGIC             SET wm_flag = "NT"
-- MAGIC             WHERE wm_flag="N";
-- MAGIC """)
-- MAGIC
-- MAGIC # exit notebook if there is not data in source table
-- MAGIC audit_log_manager.handle_source_table_empty(spark,topic,DATABRICKS_HOST,DATABRICKS_TOKEN,DBR_JOBS_API_ENDPOINT,NETWORK)
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
-- MAGIC # Insert into curated table, with DQ rules enforces
-- MAGIC df_good=spark.sql(f"""
-- MAGIC             INSERT INTO kpn_silver.network.{topic}_dt(
-- MAGIC             LINEID,
-- MAGIC             DEVICEID,
-- MAGIC             MAC,
-- MAGIC             STATIONTYPE,
-- MAGIC             LATENCYAVERAGE,
-- MAGIC             THROUGHPUTACTIVEAVERAGE,
-- MAGIC             RSSIHISTOGRAMCOUNT,
-- MAGIC             RSSIHISTOGRAMAVGTPUT,
-- MAGIC             RSSI,
-- MAGIC             CRTD_DTTM, 
-- MAGIC             CRTD_BY
-- MAGIC             )
-- MAGIC             SELECT 
-- MAGIC             LINEID,
-- MAGIC             DEVICEID,
-- MAGIC             MAC,
-- MAGIC             STATIONTYPE,
-- MAGIC             LATENCYAVERAGE,
-- MAGIC             THROUGHPUTACTIVEAVERAGE,
-- MAGIC             RSSIHISTOGRAMCOUNT,
-- MAGIC             RSSIHISTOGRAMAVGTPUT,
-- MAGIC             RSSI,
-- MAGIC             current_timestamp() AS CRTD_DTTM,
-- MAGIC             'data_engineer_group' AS CRTD_BY
-- MAGIC             FROM kpn_silver.default.{topic}_dt_temp
-- MAGIC             WHERE good_bad_flag=1 ;
-- MAGIC """)
-- MAGIC
-- MAGIC # Insert into Curated Table, with DQ Rules enforces
-- MAGIC df_bad=spark.sql(f"""
-- MAGIC             INSERT INTO kpn_silver.network.rejection_{topic}_dt (
-- MAGIC             LINEID,
-- MAGIC             DEVICEID,
-- MAGIC             MAC,
-- MAGIC             STATIONTYPE,
-- MAGIC             LATENCYAVERAGE,
-- MAGIC             THROUGHPUTACTIVEAVERAGE,
-- MAGIC             RSSIHISTOGRAMCOUNT,
-- MAGIC             RSSIHISTOGRAMAVGTPUT,
-- MAGIC             RSSI,
-- MAGIC             DQ_STATUS,
-- MAGIC             CRTD_DTTM, 
-- MAGIC             CRTD_BY
-- MAGIC             )
-- MAGIC             SELECT 
-- MAGIC             LINEID,
-- MAGIC             DEVICEID,
-- MAGIC             MAC,
-- MAGIC             STATIONTYPE,
-- MAGIC             LATENCYAVERAGE,
-- MAGIC             THROUGHPUTACTIVEAVERAGE,
-- MAGIC             RSSIHISTOGRAMCOUNT,
-- MAGIC             RSSIHISTOGRAMAVGTPUT,
-- MAGIC             RSSI,
-- MAGIC             'O' as DQ_STATUS,
-- MAGIC             current_timestamp() AS CRTD_DTTM,
-- MAGIC             'data_engineer_group' AS CRTD_BY
-- MAGIC             FROM kpn_silver.default.{topic}_dt_temp
-- MAGIC             WHERE WHERE good_bad_flag=0
-- MAGIC """)
-- MAGIC
-- MAGIC # saved number edited records for auditing
-- MAGIC rows_affected_good_table=df_good.first().num_affected_rows
-- MAGIC rows_affected_bad_table=df_bad.first().num_affected_rows
-- MAGIC spark.sql(f"""drop table kpn_silver.default.{topic}_dt_temp;""")
-- MAGIC
-- MAGIC # Updating curated records back in the source table to mark them as processed
-- MAGIC spark.sql(f"""
-- MAGIC             UPDATE kpn_bronze.network.{topic}_dt
-- MAGIC             SET wm_flag = "Y" -- Setting wm_flag to 'Y' indicates these records have been processed
-- MAGIC             WHERE wm_flag="NT";
-- MAGIC """) # Selecting records with wm_flag 'NT' to update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AUDIT

-- COMMAND ----------

-- MAGIC %python
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
-- 3. update timestamp and wm_flag="R"

-- -- 1. Rectified Records --
-- with rectified_records as (
-- select source_inhome.* from kpn_bronze.network.inhome_dt source_inhome
-- inner join kpn_silver.network.rejection_inhome_dt target_inhome
-- on source_inhome.MAC=target_inhome.MAC
-- and source_inhome.DEVICEID=target_inhome.DEVICEID
-- where wm_flag="NT"
-- )

-- -- 2. Insert Rectified recrods in the good table --
-- insert into kpn_silver.network.inhome_dt
-- select 
-- *,
-- current_timestamp() as CRTD_DTTM,
-- 'data_engineer_group' AS CRTD_BY
-- from rectified_records

-- -- 3. Status updated to Resolved --
-- merge into kpn_silver.network.rejection_inhome_dt as target_inhome
-- using rectified_records as source_inhome
-- on source_inhome.MAC=target_inhome.MAC
-- and source_inhome.DEVICEID=target_inhome.DEVICEID
-- when matched update 
-- set DQ_STATUS = 'R'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #INCREMENTAL LOAD

-- COMMAND ----------

-- MERGE INTO kpn_silver.network.inhome_dt AS t 
-- USING kpn_bronze.network.inhome_dt AS s
-- ON t.MAC = s.MAC
-- AND t.DEVICEID = s.DEVICEID
-- WHEN NOT MATCHED AND (MAC RLIKE '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$') AND (STATIONTYPE NOT RLIKE '^[0-9]+$') AND (LATENCYAVERAGE < 10) THEN
-- INSERT(
-- LINEID,
-- DEVICEID,
-- MAC,
-- STATIONTYPE,
-- LATENCYAVERAGE,
-- THROUGHPUTACTIVEAVERAGE,
-- RSSIHISTOGRAMCOUNT,
-- RSSIHISTOGRAMAVGTPUT,
-- RSSI,
-- CRTD_DTTM, 
-- CRTD_BY)

-- VALUES(
--  LINEID,
--  DEVICEID,
--  MAC,
--  STATIONTYPE,
--  LATENCYAVERAGE,
--  THROUGHPUTACTIVEAVERAGE,
--  RSSIHISTOGRAMCOUNT,
--  RSSIHISTOGRAMAVGTPUT,
--  RSSI,
--  current_timestamp() AS CRTD_DTTM,
--  'data_engineer_group' AS CRTD_BY)