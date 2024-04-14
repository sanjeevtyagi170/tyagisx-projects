-- Databricks notebook source
-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count
-- MAGIC
-- MAGIC # creating a dataframe using streaming table
-- MAGIC df_streaming_table = spark.sql("""select *  from kpn_silver.customerservice.alarm_slt""")
-- MAGIC
-- MAGIC # creating a dataframe using delta table
-- MAGIC df_delta_table = spark.sql("""select *  from kpn_silver.network.inhome_dt""")
-- MAGIC
-- MAGIC # joining both the dataframes streaming and delta table using MAC_ID as joining key and calculating number of alarm for each device
-- MAGIC final_df = df_streaming_table.join(df_delta_table,df_streaming_table.MAC_ID == df_delta_table.MAC, how = 'left').groupby('INHOME_ID','DEVICEID','MAC').agg(count('*').alias('No_of_Alarm'))
-- MAGIC
-- MAGIC display(final_df)

-- COMMAND ----------

select TIMESTAMPADD(HOUR, -1, CURRENT_TIMESTAMP())

-- COMMAND ----------

-- Time Travel
-- SELECT * FROM kpn_silver.customerservice.alarm_dt version AS OF 6800
-- Restore table kpn_silver.customerservice.alarm_dt to version as of 6800 -- Restoring a table to its previous state directly
-- SELECT * FROM kpn_silver.customerservice.alarm_dt TIMESTAMP AS OF TIMESTAMPADD(HOUR, -1, CURRENT_TIMESTAMP()) -- last hour however condition - the latest commit timestamp should be present
SELECT * FROM kpn_silver.customerservice.alarm_dt TIMESTAMP AS OF date_sub(current_date(), 1) limit 2 -- previous day

-- COMMAND ----------

-- Foreign Catalog
select * from kpn_snowflake.customer_service.breakdown_fct limit 2;

-- COMMAND ----------

ALTER TABLE kpn_silver.customerservice.alarm_dt;

SET TBLPROPERTIES ('delta.logRetentionDuration'='interval 1 HOURS', 'delta.deletedFileRetentionDuration'='interval 1 HOURS');

VACUUM kpn_silver.customerservice.alarm_dt;

-- COMMAND ----------


