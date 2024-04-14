# Databricks notebook source
# MAGIC %md
# MAGIC # IMPORTING LIBRARIES AND INITIALIZING VARIABLES

# COMMAND ----------

import datetime,requests,json,sys
sys.path.append('/Workspace/Shared/KPN-TEST/utils/')
from config import *
from urllib.parse import urljoin
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
import pyspark.sql.functions as F
from DataQualityAndJobAuditManager import DataQualityManager
from DataQualityAndJobAuditManager import AuditLogManager

# Variables
topic=ALARM
CATALOG=CUSTOMERSERVICE
user_name=dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
notebook_path=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
job_id = dbutils.widgets.get('job_id')
run_id = dbutils.widgets.get('job.run_id')


# COMMAND ----------

# Initialize AuditLogManager instance
audit_log_manager = AuditLogManager()

# Initialize DataQualityManager instance
data_quality_manager=DataQualityManager()

# COMMAND ----------

# MAGIC %md
# MAGIC #DATA LOADING AND TABLES CREATION

# COMMAND ----------

# Set the timeParserPolicy to LEGACY mode for Spark SQL
spark.sql(f"""SET spark.sql.legacy.timeParserPolicy = LEGACY;""")

# Step 1: Flag records from Bronze table, that will be used for Curation
spark.sql(f"""
          UPDATE kpn_bronze.customerservice.{topic}_dt
          SET wm_flag = "NT"
          WHERE wm_flag="N";
""")

# exit notebook if there is no new data in the source table
audit_log_manager.handle_source_table_empty(spark,topic,DATABRICKS_HOST,DATABRICKS_TOKEN,DBR_JOBS_API_ENDPOINT,CATALOG)

# extract dataquality conditions
good_condition=data_quality_manager.dq_from_information_schema_uc_bronze(spark,topic)

# Creating a temp table with the good_bad_flag
spark.sql(f"""
          create or replace table kpn_silver.default.{topic}_dt_temp
          select *,case when {good_condition} then 1 else 0 end as good_bad_flag
          from kpn_bronze.customerservice.{topic}_dt
          WHERE wm_flag='NT' 
""")

# Insert Curated into Good Table, with DQ Rules enforces
df_good=spark.sql(f"""
          INSERT INTO kpn_silver.customerservice.{topic}_dt (
              ID_ALARM, 
              DATE_TIME_NOTIF,
              DATE_TIME,
              NAME_SUB_SUPPLIER ,
              GRAVITY ,
              TIPO_NOTIF ,
              COD_LOCAL ,
              TECHNOLOGY_NAME ,
              INS_SUB_SUP ,
              OBJECT ,
              DESC_ALM ,
              ID_NOTIF ,
              MAC_ID ,
              CRTD_DTTM , 
              CRTD_BY)
          SELECT 
            ID_ALARM,
            to_timestamp(DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
            to_timestamp(DATE_TIME,'yyyy-MM-dd HH:mm:ss') AS DATE_TIME,
            NAME_SUB_SUPPLIER,
            GRAVITY,
            TIPO_NOTIF,
            COD_LOCAL,
            TECHNOLOGY_NAME,
            INS_SUB_SUP,
            OBJECT,
            DESC_ALM,
            ID_NOTIF,
            MAC_ID,
            current_timestamp() AS CRTD_DTTM,
            'data_engineer_group' AS CRTD_BY  
          FROM kpn_silver.default.{topic}_dt_temp
          WHERE good_bad_flag=1 ;
""")

# Insert Curated into Bad Table, with DQ Rules enforces
df_bad=spark.sql(f"""
        INSERT INTO kpn_silver.customerservice.rejection_{topic}_dt (
            ID_ALARM, 
            DATE_TIME_NOTIF,
            DATE_TIME,
            NAME_SUB_SUPPLIER ,
            GRAVITY ,
            TIPO_NOTIF ,
            COD_LOCAL ,
            TECHNOLOGY_NAME ,
            INS_SUB_SUP,
            OBJECT,
            DESC_ALM,
            ID_NOTIF,
            MAC_ID,
            DQ_STATUS,
            CRTD_DTTM , 
            CRTD_BY)
        SELECT 
            ID_ALARM,
            to_timestamp(DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
            to_timestamp(DATE_TIME,'yyyy-MM-dd HH:mm:ss') AS DATE_TIME,
            NAME_SUB_SUPPLIER,
            GRAVITY,
            TIPO_NOTIF,
            COD_LOCAL,
            TECHNOLOGY_NAME,
            INS_SUB_SUP,
            OBJECT,
            DESC_ALM,
            ID_NOTIF,
            MAC_ID,
            'O' as DQ_STATUS,
            current_timestamp() AS CRTD_DTTM,
            'data_engineer_group' AS CRTD_BY  
        FROM kpn_silver.default.{topic}_dt_temp
        WHERE good_bad_flag=0
""")

# Saved count to records for auditing
rows_affected_good_table=df_good.first().num_affected_rows
rows_affected_bad_table=df_bad.first().num_affected_rows
spark.sql(f"""drop table kpn_silver.default.{topic}_dt_temp;""")

# Updating curated records back in the source table to mark them as processed
spark.sql(f"""
          UPDATE kpn_bronze.customerservice.{topic}_dt
          SET wm_flag = "Y" -- Setting wm_flag to 'Y' indicates these records have been processed
          WHERE wm_flag="NT"; 
""") # Selecting records with wm_flag 'NT' to update


# COMMAND ----------

# MAGIC %md
# MAGIC # AUDIT

# COMMAND ----------

# Call methods to create and save audit tables
audit_row_df = audit_log_manager.create_audit_table_row(spark, topic, job_id, run_id, rows_affected_good_table, rows_affected_bad_table, user_name, notebook_path)
audit_row_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_tables")

job_info_df = audit_log_manager.fetch_job_info_api(spark,DATABRICKS_HOST, DATABRICKS_TOKEN,DBR_JOBS_API_ENDPOINT)
job_info_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_jobs")

# COMMAND ----------

# MAGIC %md
# MAGIC # DATA QUALITY

# COMMAND ----------



# Data Rectification Logic
#  1. Identify rectified records from the source table based on composite
#  2. Insert records good table
#  3. update timestamp and DQ_STATUS = "R"

#  1. Rectified Records 
#  spark.sql(f"""
#             CREATE OR REPLACE TEMP VIEW {topic}_rectified_records
#             AS 
#             SELECT source.* FROM kpn_silver.default.{topic}_dt_temp source
#             INNER JOIN kpn_silver.customerservice.rejection_{topic}_dt target
#             ON source.ID_ALARM = target.ID_ALARM
#             WHERE good_bad_flag=1 
#  """)

#  2. Insert Rectified recrods in the good table
#  spark.sql(f""" 
#         INSERT INTO kpn_silver.customerservice.{topic}_dt(
#             ID_ALARM, 
#             DATE_TIME_NOTIF,
#             DATE_TIME,
#             NAME_SUB_SUPPLIER ,
#             GRAVITY ,
#             TIPO_NOTIF ,
#             COD_LOCAL ,
#             TECHNOLOGY_NAME ,
#             INS_SUB_SUP ,
#             OBJECT ,
#             DESC_ALM ,
#             ID_NOTIF ,
#             MAC_ID ,
#             CRTD_DTTM, 
#             CRTD_BY)
#         SELECT 
#             ID_ALARM, 
#             DATE_TIME_NOTIF,
#             DATE_TIME,
#             NAME_SUB_SUPPLIER,
#             GRAVITY,
#             TIPO_NOTIF,
#             COD_LOCAL,
#             TECHNOLOGY_NAME,
#             INS_SUB_SUP,
#             OBJECT,
#             DESC_ALM,
#             ID_NOTIF,
#             MAC_ID,
#             current_timestamp() AS CRTD_DTTM,
#             'data_engineer_group' AS CRTD_BY
#         FROM {topic}_rectified_records 
# """)


# 3. Status updated to Resolved 
#  spark.sql(f"""
#             MERGE INTO kpn_silver.customerservice.rejection_{topic}_dt AS target
#             USING {topic}_rectified_records AS source
#             ON target.ID_alarm = source.ID_alarm
#             WHEN MATCHED THEN
#             UPDATE SET target.DQ_STATUS = 'R',target_alarm.CRTD_DTTM=current_timestamp() 
#  """)

# COMMAND ----------

# MAGIC %md
# MAGIC #INCREMENTAL LOAD

# COMMAND ----------

# spark.sql(f"""
#         MERGE INTO kpn_silver.customerservice.{topic}_dt AS t 
#         USING kpn_silver.default.{topic}_dt_temp AS s
#         ON t.ID_ALARM = s.ID_ALARM
#         WHEN NOT MATCHED AND  good_bad_flag=1 THEN
#         INSERT(
#             ID_ALARM,
#             DATE_TIME_NOTIF,
#             DATE_TIME,
#             NAME_SUB_SUPPLIER,
#             GRAVITY,
#             TIPO_NOTIF,
#             COD_LOCAL,
#             TECHNOLOGY_NAME,
#             INS_SUB_SUP,
#             OBJECT,
#             DESC_ALM,
#             ID_NOTIF,
#             MAC_ID,
#             CRTD_DTTM,
#             CRTD_BY)
#         VALUES(
#             s.ID_ALARM,
#             to_timestamp(s.DATE_TIME_NOTIF),
#             to_timestamp(s.DATE_TIME,'yyyy-MM-dd HH:mm:ss'),
#             s.NAME_SUB_SUPPLIER,s.GRAVITY,
#             s.TIPO_NOTIF,
#             s.COD_LOCAL,
#             s.TECHNOLOGY_NAME,
#             s.INS_SUB_SUP,
#             s.OBJECT,
#             s.DESC_ALM,
#             s.ID_NOTIF,
#             s.MAC_ID,
#             current_timestamp(),
#             'data_engineer_group') 
# """)