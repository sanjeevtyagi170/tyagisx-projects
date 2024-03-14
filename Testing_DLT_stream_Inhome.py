# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -------------------- Data Ingestion from Bronze Table-------------------
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE inhome_silver
# MAGIC ------------Below are all data constraints or Data quality rules-----------------
# MAGIC (
# MAGIC     CONSTRAINT valid_date EXPECT (metrics_date > "2021-01-01") ON VIOLATION FAIL UPDATE, --Sample Filtering
# MAGIC     CONSTRAINT valid_date EXPECT (metrics_date > "2021-01-01") ON VIOLATION DROP ROW, --Sample Filtering
# MAGIC     CONSTRAINT valid_id EXPECT (line_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC     CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW, -- For CDC
# MAGIC     CONSTRAINT valid_name EXPECT (deviceid IS NOT NULL or operation = "DELETE"), -- For CDC
# MAGIC     CONSTRAINT valid_address EXPECT (
# MAGIC     (address IS NOT NULL and 
# MAGIC     city IS NOT NULL and 
# MAGIC     state IS NOT NULL and 
# MAGIC     zip_code IS NOT NULL) or
# MAGIC     operation = "DELETE"), -- For CDC
# MAGIC     CONSTRAINT valid_email EXPECT (
# MAGIC     rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or 
# MAGIC     operation = "DELETE") ON VIOLATION DROP ROW
# MAGIC     )
# MAGIC ------------------------------------------------------------------------------------
# MAGIC COMMENT "Append only data with 3 data quality checks"
# MAGIC TBLPROPERTIES ("quality" = "silver")
# MAGIC AS SELECT timestamp(order_timestamp) AS order_timestamp, --* EXCEPT (order_timestamp, source_file, _rescued_data)
# MAGIC FROM STREAM(LIVE.inhome_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE inhome_gold
# MAGIC AS 
# MAGIC SELECT *, "x" as aggregation_logic
# MAGIC FROM STREAM(LIVE.inhome_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC # Quaratine Records

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE quarantine
# MAGIC (
# MAGIC   CONSTRAINT quaratine EXPECT (NOT(id > 0)) ON VIOLATION DROP ROW
# MAGIC )
# MAGIC AS SELECT *
# MAGIC FROM STREAM(<table_name>)