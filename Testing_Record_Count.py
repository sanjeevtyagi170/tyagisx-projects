# Databricks notebook source
# MAGIC %sql
# MAGIC INSERT INTO kpn_test.default.Curation_DownStream_bronze
# MAGIC VALUES('2STL-St_23Leonards','52CB17B1D5B65','2022/01/01 00:00','SWCMT0000161/00015C922072/cable-downstream 8/4/3', 1401188196,937410221,0.9298737,0.8371177,null)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpn_silver.kpn_silver_db.curation_downstream_silver_non_dlt

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kpn_silver.kpn_silver_db.curation_downstream_silver

# COMMAND ----------

display(dbutils.fs.ls('s3://uc-kpn-landing-bucket/topics/Breakdowns/year=2024/month=03/day=01/'))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`s3://uc-kpn-landing-bucket/topics/Breakdowns/year=2024/month=03/day=01/hour=10/`

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kpn_silver.kpn_silver_db.Curation_Breakdown_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kpn_silver.kpn_silver_db.Curation_Breakdown_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from kpn_silver.kpn_silver_db.Curation_Breakdown_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from parquet.`s3://uc-kpn-landing-bucket/topics/Breakdowns/year=2024/month=03/day=01/`