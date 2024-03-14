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
# MAGIC -- create temporary view kpn_test.default.results_vw
# MAGIC -- select * from kpn_test.default.results