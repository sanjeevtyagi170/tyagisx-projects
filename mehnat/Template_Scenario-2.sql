-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Bronze Layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE Curation_Breakdown_bronze
COMMENT 'Ingesting raw data from s3 bucket to bronze table'
TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT * FROM cloud_files('s3://uc-kpn-landing-bucket/topics/Breakdowns/year=2024/month=03/day=01/*','parquet')

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Silver Layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE Curation_Breakdown_silver
COMMENT "Append only data with 1 data quality checks"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  *
FROM STREAM(LIVE.Curation_Breakdown_bronze)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Gold Layer

-- COMMAND ----------

-- CREATE OR REFRESH LIVE TABLE alarm_gold
-- AS 
-- COMMENT "Append only data with 1 data quality checks"
-- TBLPROPERTIES ("quality" = "gold")
-- SELECT *, "x" as aggregation_logic
-- FROM STREAM(LIVE.`kpn-silver`.kpn_silver_db.alarm_silver)
-- select * from parquet.`s3://uc-kpn-landing-bucket/topics/Breakdowns/year=2024/month=03/day=01/hour=10/`