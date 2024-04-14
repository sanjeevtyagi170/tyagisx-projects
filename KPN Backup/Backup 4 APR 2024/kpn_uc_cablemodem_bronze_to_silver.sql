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
-- MAGIC  # Variables
-- MAGIC topic=CABLEMODEM
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
-- MAGIC         max_end_time_job_df = df_filtered[df_filtered['job_name'] == 'kpn_uc_cablemodem_bronze_to_silver_job']
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

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DATA LOADING

-- COMMAND ----------

-- Set the timeParserPolicy to LEGACY mode for Spark SQL
SET spark.sql.legacy.timeParserPolicy = LEGACY;

-- Step 1: Flag records from Bronze table, that will be used for Curation
-- Update records where the wm_flag column equals "N" to "NT" to indicate they are flagged for curation.
-- SOURCE TABLE: kpn_bronze.network.cablemodem_dt
UPDATE kpn_bronze.network.cablemodem_dt
SET wm_flag = "NT"
WHERE wm_flag="N";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # if there is no new data found in the source data table the exit from notebook
-- MAGIC try:
-- MAGIC     # Check if data is present
-- MAGIC     df = spark.sql("""select * from kpn_bronze.network.cablemodem_dt where wm_flag='NT'""")
-- MAGIC     if df.count() == 0:
-- MAGIC         raise Exception("No data found in the source table")
-- MAGIC except Exception as e:
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
-- MAGIC # spark.sql(f"""select * from kpn_silver.default.{topic}_dt_temp""").groupby('good_bad_flag').count().show()

-- COMMAND ----------

-- STEP 2: Data Rectification Logic
-- 1. Identify rectified records from the source table based on composite
-- 2. Insert records good table
-- 3. update timestamp and DQ_STATUS = "R"

-- -- 1. Rectified Records --
-- spark.sql(f"""CREATE OR REPLACE TEMP VIEW cablemodem_rectified_records
-- AS 
-- SELECT source_cablemodem.* FROM kpn_silver.default.{topic}_dt_temp source_cablemodem
-- INNER JOIN kpn_silver.network.rejection_cablemodem_dt target_cablemodem
-- ON source_cablemodem.HUB=target_cablemodem.HUB
-- AND source_cablemodem.RX_POWER_DN=target_cablemodem.RX_POWER_DN
-- WHERE good_bad_flag = 1  """)

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
-- MAGIC #TABLES CREATION

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- Step 3: Insert into Curated Table, with DQ Rules enforces
-- MAGIC # -- GOOD TABLE:kpn_silver.network.cablemodem_dt
-- MAGIC # -- DQ RULES
-- MAGIC # -- 1. MAC should not be NULL
-- MAGIC # -- 2. Should be in YYYY-MM-DD HH:MM:SS format only
-- MAGIC df_good=spark.sql(f"""
-- MAGIC INSERT INTO kpn_silver.network.cablemodem_dt(
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
-- MAGIC # FROM kpn_bronze.network.cablemodem_dt
-- MAGIC # WHERE wm_flag="NT" 
-- MAGIC # AND METRICS_DATE RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$')
-- MAGIC # AND MAC_ADDR != ''

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- Step 4: Insert into Curated Table, with DQ Rules enforces
-- MAGIC # -- BAD TABLE:kpn_silver.network.rejection_cablemodem_dt
-- MAGIC # -- DQ RULES
-- MAGIC # -- 1. MAC should not be NULL
-- MAGIC # -- 2. Should be in YYYY-MM-DD HH:MM:SS format only
-- MAGIC df_bad=spark.sql(f"""
-- MAGIC INSERT INTO kpn_silver.network.rejection_cablemodem_dt (
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
-- MAGIC
-- MAGIC # FROM kpn_bronze.network.cablemodem_dt
-- MAGIC # WHERE wm_flag="NT" 
-- MAGIC # AND ( MAC_ADDR = '' OR 
-- MAGIC # METRICS_DATE NOT RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # saved number edited records for auditing
-- MAGIC rows_affected_good_table=df_good.first().num_affected_rows
-- MAGIC rows_affected_bad_table=df_bad.first().num_affected_rows
-- MAGIC spark.sql(f"""drop table kpn_silver.default.{topic}_dt_temp;""")

-- COMMAND ----------

--STEP 5: Updating curated records back in the source table to mark them as processed
UPDATE kpn_bronze.network.cablemodem_dt
SET wm_flag = "Y" -- Setting wm_flag to 'Y' indicates these records have been processed
WHERE wm_flag="NT"; -- Selecting records with wm_flag 'NT' to update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #AUDIT

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # Call methods to create and save audit tables
-- MAGIC audit_row_df = audit_log_manager.create_audit_table_row(topic, job_id, run_id, rows_affected_good_table, rows_affected_bad_table, user_name, notebook_path)
-- MAGIC audit_row_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_tables")
-- MAGIC
-- MAGIC job_info_df = audit_log_manager.fetch_job_info_api(DATABRICKS_HOST, DATABRICKS_TOKEN)
-- MAGIC job_info_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_jobs")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #INCREMENTAL LOAD

-- COMMAND ----------

-- spark.sql(f""" MERGE INTO kpn_silver.network.cablemodem_dt AS t 
-- USING kpn_silver.default.{topic}_dt_temp AS s
-- ON t.HUB = s.HUB 
-- AND t.RX_POWER_DN = s.RX_POWER_DN
-- WHEN NOT MATCHED AND good_bad_flag=1 THEN
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
--  'data_engineer_group') """)
