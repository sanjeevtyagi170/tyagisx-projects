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
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC # Variables
-- MAGIC topic=INHOME
-- MAGIC user_name=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
-- MAGIC notebook_path=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
-- MAGIC job_id = dbutils.widgets.get('job_id')
-- MAGIC run_id = dbutils.widgets.get('job.run_id')

-- COMMAND ----------

-- MAGIC %python
-- MAGIC class AuditLogManager:
-- MAGIC     def create_audit_table_row(self, topic, job_id, run_id, rows_affected_good_table, rows_affected_bad_table, user_name, notebook_path):
-- MAGIC         schema = StructType([
-- MAGIC             StructField('topic', StringType(), True),
-- MAGIC             StructField('job_id', StringType(), True),
-- MAGIC             StructField('run_id', StringType(), True),
-- MAGIC             StructField('rows_affected_good_table', LongType(), True),
-- MAGIC             StructField('rows_affected_bad_table', LongType(), True),
-- MAGIC             StructField('user_name', StringType(), True),
-- MAGIC             StructField('notebook_path', StringType(), True)
-- MAGIC         ])
-- MAGIC         data = [(topic, job_id, run_id, rows_affected_good_table, rows_affected_bad_table, user_name, notebook_path)]
-- MAGIC         return spark.createDataFrame(data, schema=schema)
-- MAGIC
-- MAGIC     def fetch_job_info_api(self, databricks_host, databricks_token):
-- MAGIC         url = urljoin(databricks_host, DBR_JOBS_API_ENDPOINT)
-- MAGIC         headers = {"Authorization": f"Bearer {databricks_token}"}
-- MAGIC         response = requests.get(url, headers=headers)
-- MAGIC         df = pd.json_normalize(response.json()['runs'])
-- MAGIC         df_filtered = df.iloc[:,[0, 1, 5, 9, 12, 17,19]]
-- MAGIC         df_filtered.columns = ["job_id", "run_id", "job_start_time", "job_end_time", "job_name", "job_status1","job_status2"]
-- MAGIC         df_filtered['job_start_time'] = pd.to_datetime(df_filtered['job_start_time'] / 1000, unit='s')
-- MAGIC         df_filtered['job_end_time'] = pd.to_datetime(df_filtered['job_end_time'] / 1000, unit='s')
-- MAGIC         max_end_time_job_df = df_filtered[df_filtered['job_name'] == 'kpn_uc_inhome_bronze_to_silver_job']
-- MAGIC         max_end_time_job_df = max_end_time_job_df[max_end_time_job_df['job_end_time'] == max_end_time_job_df['job_end_time'].max()]
-- MAGIC         schema = StructType([
-- MAGIC             StructField('job_id', LongType(), True),
-- MAGIC             StructField('run_id', LongType(), True),
-- MAGIC             StructField('job_start_time', TimestampType(), True),
-- MAGIC             StructField('job_end_time', TimestampType(), True),
-- MAGIC             StructField('job_name', StringType(), True),
-- MAGIC             StructField('job_status1', StringType(), True),
-- MAGIC             StructField('job_status2', StringType(), True)
-- MAGIC         ])
-- MAGIC         return spark.createDataFrame(max_end_time_job_df, schema=schema)
-- MAGIC
-- MAGIC # Initialize AuditLogManager instance
-- MAGIC audit_log_manager = AuditLogManager()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DATA LOADING

-- COMMAND ----------

-- Set the timeParserPolicy to LEGACY mode for Spark SQL
SET spark.sql.legacy.timeParserPolicy = LEGACY;

-- Step 1: Flag records from Bronze table, that will be used for Curation
-- Update records where the wm_flag column equals "N" to "NT" to indicate they are flagged for curation.
-- SOURCE TABLE: kpn_bronze.network.inhome_dt
UPDATE kpn_bronze.network.inhome_dt
SET wm_flag = "NT"
WHERE wm_flag="N";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # if there is no new data found in the source data table the exit from notebook
-- MAGIC try:
-- MAGIC     # Check if data is present
-- MAGIC     df = spark.sql("""select * from kpn_bronze.network.inhome_dt where wm_flag='NT'""")
-- MAGIC     if df.count() == 0:
-- MAGIC         raise Exception("No data found in the source table")
-- MAGIC except Exception as e:
-- MAGIC     
-- MAGIC     # Handle the exception
-- MAGIC     print("Exception:", e)
-- MAGIC     job_info_df = audit_log_manager.fetch_job_info_api(DATABRICKS_HOST, DATABRICKS_TOKEN)
-- MAGIC     job_info_df = job_info_df.withColumn('job_status2', F.lit("No data found in the source table"))
-- MAGIC     job_info_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_jobs")
-- MAGIC
-- MAGIC     # Optionally, you can exit the notebook
-- MAGIC     dbutils.notebook.exit("No data found in the source table")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #DATA QUALITY

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Extracting condition from the CSV file
-- MAGIC df=pd.read_csv(CSVPATH)
-- MAGIC row=df[(df['dataset_name']==topic) & (df['dq_flag'].str.lower()=="enable")]['condition']
-- MAGIC good_condition=' '.join(map(str, row))
-- MAGIC
-- MAGIC # Creating a temp table with the good_bad_flag
-- MAGIC spark.sql(f"""create or replace table kpn_silver.default.{topic}_dt_temp
-- MAGIC select *,case when {good_condition[3:]} then 1 else 0 end as good_bad_flag
-- MAGIC from kpn_bronze.network.{topic}_dt
-- MAGIC WHERE wm_flag='NT' """)
-- MAGIC
-- MAGIC # Debugging
-- MAGIC # spark.sql("""select * from kpn_silver.default.inhome_dt_temp""").groupby('good_bad_flag').count().show()

-- COMMAND ----------

-- STEP 2 Data Rectification Logic
-- 1. Identify rectified records from the source table based on composite
-- 2. Insert records good table
-- 3. update timestamp and DQ_STATUS = "R"

-- -- 1. Rectified Records --
-- spark.sql(f""" CREATE OR REPLACE TEMP VIEW inhome_rectified_records
-- AS 
-- SELECT source_inhome.* FROM kpn_silver.default.{inhome}_dt_temp source_inhome
-- INNER JOIN kpn_silver.customerservice.rejection_inhome_dt target_inhome
-- on source_inhome.MAC=target_inhome.MAC
-- and source_inhome.DEVICEID=target_inhome.DEVICEID
-- WHERE good_bad_flag=1 """)

-- -- 2. Insert Rectified recrods in the good table --
-- INSERT INTO kpn_silver.customerservice.inhome_dt(
--   ID_inhome, 
--   DATE_TIME_NOTIF,
--   DATE_TIME,
--   NAME_SUB_SUPPLIER ,
--   GRAVITY ,
--   TIPO_NOTIF ,
--   COD_LOCAL ,
--   TECHNOLOGY_NAME ,
--   INS_SUB_SUP ,
--   OBJECT ,
--   DESC_ALM ,
--   ID_NOTIF ,
--   MAC_ID ,
--   CRTD_DTTM, 
--   CRTD_BY)
-- SELECT 
--   ID_inhome, 
--   DATE_TIME_NOTIF,
--   DATE_TIME,
--   NAME_SUB_SUPPLIER,
--   GRAVITY,
--   TIPO_NOTIF,
--   COD_LOCAL,
--   TECHNOLOGY_NAME,
--   INS_SUB_SUP,
--   OBJECT,
--   DESC_ALM,
--   ID_NOTIF,
--   MAC_ID,
--   current_timestamp() AS CRTD_DTTM,
--   'data_engineer_group' AS CRTD_BY
-- FROM inhome_rectified_records


-- -- 3. Status updated to Resolved --
-- MERGE INTO kpn_silver.customerservice.rejection_inhome_dt AS target_inhome
-- USING inhome_rectified_records AS source_inhome
-- ON source_inhome.MAC=target_inhome.MAC
-- AND source_inhome.DEVICEID=target_inhome.DEVICEID
-- WHEN MATCHED THEN
-- UPDATE SET target_inhome.DQ_STATUS = 'R',target_inhome.CRTD_DTTM=current_timestamp()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TABLES CREATION

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- Step 3: Insert into curated table, with DQ rules enforces
-- MAGIC # -- GOOD TABLE:kpn_silver.network.inhome_dt
-- MAGIC # -- DQ RULES
-- MAGIC # -- 1. MAC should be in XX:XX:XX:XX:XX:XX format only
-- MAGIC # -- 2. Station type should not be Numeric 
-- MAGIC # -- 3. Latency average should be <10 only
-- MAGIC df_good=spark.sql(f"""
-- MAGIC INSERT INTO kpn_silver.network.inhome_dt(
-- MAGIC   LINEID,
-- MAGIC   DEVICEID,
-- MAGIC   MAC,
-- MAGIC   STATIONTYPE,
-- MAGIC   LATENCYAVERAGE,
-- MAGIC   THROUGHPUTACTIVEAVERAGE,
-- MAGIC   RSSIHISTOGRAMCOUNT,
-- MAGIC   RSSIHISTOGRAMAVGTPUT,
-- MAGIC   RSSI,
-- MAGIC   CRTD_DTTM, 
-- MAGIC   CRTD_BY
-- MAGIC )
-- MAGIC SELECT 
-- MAGIC   LINEID,
-- MAGIC   DEVICEID,
-- MAGIC   MAC,
-- MAGIC   STATIONTYPE,
-- MAGIC   LATENCYAVERAGE,
-- MAGIC   THROUGHPUTACTIVEAVERAGE,
-- MAGIC   RSSIHISTOGRAMCOUNT,
-- MAGIC   RSSIHISTOGRAMAVGTPUT,
-- MAGIC   RSSI,
-- MAGIC   current_timestamp() AS CRTD_DTTM,
-- MAGIC   'data_engineer_group' AS CRTD_BY
-- MAGIC FROM kpn_silver.default.{topic}_dt_temp
-- MAGIC WHERE good_bad_flag=1 ;""")
-- MAGIC
-- MAGIC
-- MAGIC # -- FROM kpn_bronze.network.inhome_dt
-- MAGIC # -- WHERE wm_flag="NT" --Flag used to identify records for insertion
-- MAGIC # -- AND (MAC RLIKE '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$') 
-- MAGIC # -- AND (STATIONTYPE NOT RLIKE '^[0-9]+$') 
-- MAGIC # -- AND (LATENCYAVERAGE < 10)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- Step 4: Insert into Curated Table, with DQ Rules enforces
-- MAGIC # -- BAD TABLE: kpn_silver.network.rejection_inhome_dt
-- MAGIC # -- DQ RULES
-- MAGIC # -- 1. MAC should be in XX:XX:XX:XX:XX:XX format only
-- MAGIC # -- 2. Station type should not be Numeric 
-- MAGIC # -- 3. Latency average should be <10 only
-- MAGIC df_bad=spark.sql(f"""
-- MAGIC INSERT INTO kpn_silver.network.rejection_inhome_dt (
-- MAGIC   LINEID,
-- MAGIC   DEVICEID,
-- MAGIC   MAC,
-- MAGIC   STATIONTYPE,
-- MAGIC   LATENCYAVERAGE,
-- MAGIC   THROUGHPUTACTIVEAVERAGE,
-- MAGIC   RSSIHISTOGRAMCOUNT,
-- MAGIC   RSSIHISTOGRAMAVGTPUT,
-- MAGIC   RSSI,
-- MAGIC   DQ_STATUS,
-- MAGIC   CRTD_DTTM, 
-- MAGIC   CRTD_BY
-- MAGIC )
-- MAGIC SELECT 
-- MAGIC   LINEID,
-- MAGIC   DEVICEID,
-- MAGIC   MAC,
-- MAGIC   STATIONTYPE,
-- MAGIC   LATENCYAVERAGE,
-- MAGIC   THROUGHPUTACTIVEAVERAGE,
-- MAGIC   RSSIHISTOGRAMCOUNT,
-- MAGIC   RSSIHISTOGRAMAVGTPUT,
-- MAGIC   RSSI,
-- MAGIC   'O' as DQ_STATUS,
-- MAGIC   current_timestamp() AS CRTD_DTTM,
-- MAGIC   'data_engineer_group' AS CRTD_BY
-- MAGIC   FROM kpn_silver.default.{topic}_dt_temp
-- MAGIC WHERE WHERE good_bad_flag=0
-- MAGIC """)
-- MAGIC
-- MAGIC
-- MAGIC # FROM kpn_bronze.network.inhome_dt
-- MAGIC # WHERE wm_flag="NT" 
-- MAGIC # AND ((MAC NOT RLIKE '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$') 
-- MAGIC # OR (STATIONTYPE RLIKE '^[0-9]+$')
-- MAGIC # OR (LATENCYAVERAGE >= 10))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # saved number edited records for auditing
-- MAGIC rows_affected_good_table=df_good.first().num_affected_rows
-- MAGIC rows_affected_bad_table=df_bad.first().num_affected_rows
-- MAGIC spark.sql(f"""drop table kpn_silver.default.{topic}_dt_temp;""")

-- COMMAND ----------

--STEP 5: Updating curated records back in the source table to mark them as processed
UPDATE kpn_bronze.network.inhome_dt
SET wm_flag = "Y" -- Setting wm_flag to 'Y' indicates these records have been processed
WHERE wm_flag="NT"; -- Selecting records with wm_flag 'NT' to update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AUDIT

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Call methods to create and save audit tables
-- MAGIC audit_row_df = audit_log_manager.create_audit_table_row(topic, job_id, run_id, rows_affected_good_table, rows_affected_bad_table, user_name, notebook_path)
-- MAGIC audit_row_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_tables")
-- MAGIC
-- MAGIC job_info_df = audit_log_manager.fetch_job_info_api(DATABRICKS_HOST, DATABRICKS_TOKEN)
-- MAGIC job_info_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_jobs")
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #INCREMENTAL LOAD

-- COMMAND ----------

-- spark.sql(f"""MERGE INTO kpn_silver.network.inhome_dt AS t 
-- USING kpn_silver.default.{topic}_dt_temp AS s
-- ON t.MAC = s.MAC
-- AND t.DEVICEID = s.DEVICEID
-- WHEN NOT MATCHED AND good_bad_flag=1  THEN
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
--  'data_engineer_group' AS CRTD_BY) """)
