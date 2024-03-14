-- Databricks notebook source
-- %python
-- from pyspark.sql.types import StructField, StructType,StringType, IntegerType
-- file_location="s3://uc-kpn-landing-bucket/batch/"
-- Checkpoint_location="s3://uc-kpn-silver-bucket/batch/Checkpoint/"
-- schema=StructType([
--     StructField("NUM_HSI",IntegerType(),True),
--     StructField("COD_AC",StringType(),True),
--     StructField("COD_GR_ELEM",StringType(),True),
--     StructField("DESIGNACAO", StringType(), True),
--     StructField("COD_PT", StringType(), True)
--   ])

-- COMMAND ----------

-- %sql
-- -- COPY INTO
-- CREATE OR REPLACE TABLE kpn_silver.kpn_silver_db.curation_topology(
-- NUM_HSI BIGINT,
-- COD_AC STRING,
-- COD_GR_ELEM STRING,
-- DESIGNACAO STRING,
-- COD_PT STRING
-- );

-- COPY INTO kpn_silver.kpn_silver_db.curation_topology
-- FROM "s3://uc-kpn-landing-bucket/batch/"
-- FILEFORMAT = CSV
-- FORMAT_OPTIONS ('mergeSchema' = 'true')
-- COPY_OPTIONS ('mergeSchema' = 'true');

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Auto Loader
-- MAGIC # spark.readStream\
-- MAGIC #     .format("cloudFiles")\
-- MAGIC #     .option("cloudFiles.format","csv")\
-- MAGIC #     .option("cloudFiles.schemaLocation",Checkpoint_location)\
-- MAGIC #     .load(file_location)\
-- MAGIC #     .writeStream\
-- MAGIC #     .option("checkpointLocation",Checkpoint_location)\
-- MAGIC #     .trigger(availableNow=True)\
-- MAGIC #     .option("mergeSchema","true")\
-- MAGIC #     .table("kpn_silver.kpn_silver_db.curation_topology")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Housekeeping
-- MAGIC # dbutils.fs.ls("s3://uc-kpn-landing-bucket/batch/Checkpoint")
-- MAGIC # dbutils.fs.rm("s3://uc-kpn-landing-bucket/batch/Checkpoint/",True)

-- COMMAND ----------

select * from kpn_silver.kpn_silver_db.curation_topology_silver

-- COMMAND ----------

insert into kpn_test.default.curation_topology_bronze
values(1503046975,'41KR67','CONC_DLUE','Concentrador DLU','SR-ST7')

-- COMMAND ----------

select * from kpn_test.default.curation_topology_bronze
where NUM_HSI = 1503046975
and COD_AC = '41KR67'
and COD_GR_ELEM = 'CONC_DLUE'

-- COMMAND ----------

select * from kpn_silver.kpn_silver_db.curation_topology_silver
where NUM_HSI = 1503046975
and COD_AC = '41KR67'
and COD_GR_ELEM = 'CONC_DLUE'

-- COMMAND ----------

select * from kpn_silver.kpn_silver_db.curation_topology_silver
where NUM_HSI = 1503046975
and COD_AC = '41KR67'
and COD_GR_ELEM = 'CONC_DLUE'

-- COMMAND ----------

select count(*) from kpn_silver.kpn_silver_db.curation_topology_silver