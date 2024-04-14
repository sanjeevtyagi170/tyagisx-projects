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
-- MAGIC topic=ALARM
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
-- MAGIC         max_end_time_job_df = df_filtered[df_filtered['job_name'] == 'kpn_uc_alarm_bronze_to_silver_job']
-- MAGIC         max_end_time_job_df = max_end_time_job_df[max_end_time_job_df['job_end_time'] == max(max_end_time_job_df['job_end_time'])]
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
-- MAGIC # Debugging
-- MAGIC # job_info_df = audit_log_manager.fetch_job_info_api(DATABRICKS_HOST, DATABRICKS_TOKEN)
-- MAGIC # # job_info_df = job_info_df.withColumn('job_status2', F.lit("No data found in the source table"))
-- MAGIC # job_info_df.show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # DATA LOADING

-- COMMAND ----------

-- Set the timeParserPolicy to LEGACY mode for Spark SQL
SET spark.sql.legacy.timeParserPolicy = LEGACY;

-- Step 1: Flag records from Bronze table, that will be used for Curation
-- Update records where the wm_flag column equals "N" to "NT" to indicate they are flagged for curation.
-- SOURCE TABLE: kpn_bronze.customerservice.alarm_dt
UPDATE kpn_bronze.customerservice.alarm_dt
SET wm_flag = "NT"
WHERE wm_flag="N";

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # if there is no new data found in the source data table the exit from notebook
-- MAGIC try:
-- MAGIC     # Check if data is present
-- MAGIC     df = spark.sql("""select * from kpn_bronze.customerservice.alarm_dt where wm_flag='NT'""")
-- MAGIC     if df.count() == 0:
-- MAGIC         raise Exception("No data found in the source table")
-- MAGIC except Exception as e:
-- MAGIC     # Handle the exception and log it into the audit table
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
-- MAGIC from kpn_bronze.customerservice.{topic}_dt
-- MAGIC WHERE wm_flag='NT' """)
-- MAGIC
-- MAGIC # Debugging
-- MAGIC # spark.sql("""select * from kpn_silver.default.alarm_dt_temp""").groupby('good_bad_flag').count().show()

-- COMMAND ----------

-- STEP 2 Data Rectification Logic
-- 1. Identify rectified records from the source table based on composite
-- 2. Insert records good table
-- 3. update timestamp and DQ_STATUS = "R"

-- -- 1. Rectified Records --
-- spark.sql(f""" CREATE OR REPLACE TEMP VIEW {topic}_rectified_records
-- AS 
-- SELECT source_alarm.* FROM kpn_silver.default.{topic}_dt_temp source_alarm
-- INNER JOIN kpn_silver.customerservice.rejection_{topic}_dt target_alarm
-- ON source_alarm.ID_ALARM = target_alarm.ID_ALARM
-- WHERE good_bad_flag=1 """)

-- -- 2. Insert Rectified recrods in the good table --
-- INSERT INTO kpn_silver.customerservice.alarm_dt(
--   ID_ALARM, 
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
--   ID_ALARM, 
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
-- FROM alarm_rectified_records


-- -- 3. Status updated to Resolved --
-- MERGE INTO kpn_silver.customerservice.rejection_alarm_dt AS target_alarm
-- USING alarm_rectified_records AS source_alarm
-- ON target_alarm.ID_alarm = source_alarm.ID_alarm
-- WHEN MATCHED THEN
-- UPDATE SET target_alarm.DQ_STATUS = 'R',target_alarm.CRTD_DTTM=current_timestamp()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #TABLES CREATION

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- Step 3: Insert into Curated Table, with DQ Rules enforces
-- MAGIC # -- GOOD TABLE: kpn_silver.customerservice.alarm_dt
-- MAGIC # -- DQ RULES
-- MAGIC # -- 1. Alarm notification datetime should not be NULL
-- MAGIC # -- 2. Alarm notification type should not be more than 1 character
-- MAGIC df_good=spark.sql(f"""
-- MAGIC INSERT INTO kpn_silver.customerservice.alarm_dt (
-- MAGIC     ID_ALARM, 
-- MAGIC     DATE_TIME_NOTIF,
-- MAGIC     DATE_TIME,
-- MAGIC     NAME_SUB_SUPPLIER ,
-- MAGIC     GRAVITY ,
-- MAGIC     TIPO_NOTIF ,
-- MAGIC     COD_LOCAL ,
-- MAGIC     TECHNOLOGY_NAME ,
-- MAGIC     INS_SUB_SUP ,
-- MAGIC     OBJECT ,
-- MAGIC     DESC_ALM ,
-- MAGIC     ID_NOTIF ,
-- MAGIC     MAC_ID ,
-- MAGIC     CRTD_DTTM , 
-- MAGIC     CRTD_BY)
-- MAGIC SELECT 
-- MAGIC   ID_ALARM,
-- MAGIC   to_timestamp(DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
-- MAGIC   to_timestamp(DATE_TIME,'yyyy-MM-dd HH:mm:ss') AS DATE_TIME,
-- MAGIC   NAME_SUB_SUPPLIER,
-- MAGIC   GRAVITY,
-- MAGIC   TIPO_NOTIF,
-- MAGIC   COD_LOCAL,
-- MAGIC   TECHNOLOGY_NAME,
-- MAGIC   INS_SUB_SUP,
-- MAGIC   OBJECT,
-- MAGIC   DESC_ALM,
-- MAGIC   ID_NOTIF,
-- MAGIC   MAC_ID,
-- MAGIC   current_timestamp() AS CRTD_DTTM,
-- MAGIC   'data_engineer_group' AS CRTD_BY  
-- MAGIC FROM kpn_silver.default.{topic}_dt_temp
-- MAGIC WHERE good_bad_flag=1 ;""")
-- MAGIC
-- MAGIC # AND (CAST(DATE_TIME_NOTIF AS TIMESTAMP) IS NOT NULL   
-- MAGIC # AND LEN(TIPO_NOTIF)=1)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # -- Step 4: Insert into Curated Table, with DQ Rules enforces
-- MAGIC # -- BAD TABLE: kpn_silver.customerservice.rejection_alarm_dt
-- MAGIC # -- DQ RULES
-- MAGIC # -- 1. Alarm notification datetime should not be NULL
-- MAGIC # -- 2. Alarm notification type should not be more than 1 character
-- MAGIC df_bad=spark.sql(f"""
-- MAGIC INSERT INTO kpn_silver.customerservice.rejection_alarm_dt (
-- MAGIC     ID_ALARM, 
-- MAGIC     DATE_TIME_NOTIF,
-- MAGIC     DATE_TIME,
-- MAGIC     NAME_SUB_SUPPLIER ,
-- MAGIC     GRAVITY ,
-- MAGIC     TIPO_NOTIF ,
-- MAGIC     COD_LOCAL ,
-- MAGIC     TECHNOLOGY_NAME ,
-- MAGIC     INS_SUB_SUP,
-- MAGIC     OBJECT,
-- MAGIC     DESC_ALM,
-- MAGIC     ID_NOTIF,
-- MAGIC     MAC_ID,
-- MAGIC     DQ_STATUS,
-- MAGIC     CRTD_DTTM , 
-- MAGIC     CRTD_BY)
-- MAGIC SELECT 
-- MAGIC     ID_ALARM,
-- MAGIC     to_timestamp(DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
-- MAGIC     to_timestamp(DATE_TIME,'yyyy-MM-dd HH:mm:ss') AS DATE_TIME,
-- MAGIC     NAME_SUB_SUPPLIER,
-- MAGIC     GRAVITY,
-- MAGIC     TIPO_NOTIF,
-- MAGIC     COD_LOCAL,
-- MAGIC     TECHNOLOGY_NAME,
-- MAGIC     INS_SUB_SUP,
-- MAGIC     OBJECT,
-- MAGIC     DESC_ALM,
-- MAGIC     ID_NOTIF,
-- MAGIC     MAC_ID,
-- MAGIC     'O' as DQ_STATUS,
-- MAGIC     current_timestamp() AS CRTD_DTTM,
-- MAGIC     'data_engineer_group' AS CRTD_BY  
-- MAGIC FROM kpn_silver.default.{topic}_dt_temp
-- MAGIC WHERE good_bad_flag=0
-- MAGIC """)
-- MAGIC # ((CAST(DATE_TIME_NOTIF AS TIMESTAMP) IS NULL 
-- MAGIC # OR LEN(TIPO_NOTIF)>1))
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # saved number edited records for auditing
-- MAGIC rows_affected_good_table=df_good.first().num_affected_rows
-- MAGIC rows_affected_bad_table=df_bad.first().num_affected_rows
-- MAGIC spark.sql(f"""drop table kpn_silver.default.{topic}_dt_temp;""")

-- COMMAND ----------

--STEP 5: Updating curated records back in the source table to mark them as processed
UPDATE kpn_bronze.customerservice.alarm_dt
SET wm_flag = "Y" -- Setting wm_flag to 'Y' indicates these records have been processed
WHERE wm_flag="NT"; -- Selecting records with wm_flag 'NT' to update

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # AUDIT

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

-- spark.sql(f"""MERGE INTO kpn_silver.customerservice.{topic}_dt AS t 
-- USING kpn_silver.default.{topic}_dt_temp AS s
-- ON t.ID_ALARM = s.ID_ALARM
-- WHEN NOT MATCHED AND  good_bad_flag=1 THEN
-- INSERT(
--  ID_ALARM,
--  DATE_TIME_NOTIF,
--  DATE_TIME,
--  NAME_SUB_SUPPLIER,
--  GRAVITY,
--  TIPO_NOTIF,
--  COD_LOCAL,
--  TECHNOLOGY_NAME,
--  INS_SUB_SUP,
--  OBJECT,
--  DESC_ALM,
--  ID_NOTIF,
--  MAC_ID,
--  CRTD_DTTM,
--  CRTD_BY)
-- VALUES(
--  s.ID_ALARM,
--  to_timestamp(s.DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
--  to_timestamp(s.DATE_TIME,'yyyy-MM-dd HH:mm:ss') AS DATE_TIME,
--  s.NAME_SUB_SUPPLIER,s.GRAVITY,
--  s.TIPO_NOTIF,
--  s.COD_LOCAL,
--  s.TECHNOLOGY_NAME,
--  s.INS_SUB_SUP,
--  s.OBJECT,
--  s.DESC_ALM,
--  s.ID_NOTIF,
--  s.MAC_ID,
--  current_timestamp(),
--  'data_engineer_group') """)