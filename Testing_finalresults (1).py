# Databricks notebook source
df=spark.sql("""select 
current_timestamp() AS CRTD_DTTM,
'Bronze' as layer,
(select count(*) from kpn_bronze.network.cablemodem) as cablemodem,
(select count(*) from kpn_bronze.customerservice.alarm) as alarm,
(select count(*) from kpn_bronze.customerservice.breakdown) as breakdown,
(select count(*) from kpn_bronze.network.inhome) as inhome,
(select count(*) from kpn_bronze.network.topology) as topology
union
select 
current_timestamp() AS CRTD_DTTM,
'Silver' as layer,
(select count(*) from kpn_silver.network.cablemodem) as cablemodem,
(select count(*) from kpn_silver.customerservice.alarm) as alarm,
(select count(*) from kpn_silver.customerservice.breakdown) as breakdown,
(select count(*) from kpn_silver.network.inhome) as inhome,
(select count(*) from kpn_silver.network.topology) as topology""")
df.write.mode("append").saveAsTable("kpn_test.default.results")

# COMMAND ----------

# %sql
# ALTER TABLE kpn_test.default.results
# ADD column CRTD_DTTM timestamp;

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view results_vw
# MAGIC as 
# MAGIC select
# MAGIC CRTD_DTTM,
# MAGIC layer,
# MAGIC cablemodem,
# MAGIC alarm,
# MAGIC breakdown,
# MAGIC inhome,
# MAGIC topology
# MAGIC from kpn_test.default.results
# MAGIC where CRTD_DTTM is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from results_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table kpn_test.default.results
# MAGIC as
# MAGIC select 
# MAGIC *
# MAGIC from results_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC *
# MAGIC from kpn_test.default.results
# MAGIC