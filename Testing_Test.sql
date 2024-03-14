-- Databricks notebook source
-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('s3://uc-kpn-landing-bucket/customerservice/breakdown/year=2024/month=03/day=06/hour=09/'))

-- COMMAND ----------

select count(*) from parquet.`s3://uc-kpn-landing-bucket/customerservice/breakdown/*/*/*/*/*.parquet`

-- COMMAND ----------

select count(*) from kpn_bronze.customerservice.breakdown

-- COMMAND ----------

select count(*) from kpn_silver.customerservice.breakdown

-- COMMAND ----------

select count(*) from kpn_silver.customerservice.rejection_breakdown

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Batch

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls('s3://uc-kpn-landing-bucket/batch/'))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.option('header',True).csv('s3://uc-kpn-landing-bucket/batch/')
-- MAGIC df.createOrReplaceTempView('topology_vw')

-- COMMAND ----------

select * from topology_vw

-- COMMAND ----------

select count(*) from kpn_bronze.network.topology

-- COMMAND ----------

select count(*) from kpn_silver.network.topology

-- COMMAND ----------

select * from kpn_bronze.network.topology
where COD_AC is null

-- COMMAND ----------

select * from kpn_bronze.network.topology
WHERE COD_GR_ELEM RLIKE '[^\\d.-]' AND LEN(COD_GR_ELEM) <= 12

-- COMMAND ----------

select * from kpn_silver.network.topology

-- COMMAND ----------

select * from kpn_bronze.network.topology s
left join kpn_silver.network.topology t
on --t.NUM_HSI = CAST(s.NUM_HSI AS BIGINT)
 s.COD_AC = t.COD_AC
--AND t.COD_GR_ELEM = s.COD_GR_ELEM
--AND t.DESIGNATION = s.DESIGNATION
--AND t.COD_PT = s.COD_PT

where s.COD_AC != t.COD_AC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Alarm

-- COMMAND ----------

select to_timestamp(date_time,'MM/dd/yyyy hh:mm a') AS date_time from kpn_bronze.customerservice.alarm

-- COMMAND ----------

select * from kpn_bronze.customerservice.alarm
where TIPO_NOTIF not RLIKE '^[A-Z]$'

-- COMMAND ----------

select count(*) from kpn_silver.customerservice.alarm

-- COMMAND ----------

select count(*) from kpn_silver.customerservice.rejection_alarm

-- COMMAND ----------

select count(*) from kpn_bronze.customerservice.alarm
where date_time_notif RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}$')

-- COMMAND ----------

select * from kpn_silver.customerservice.alarm
--where CAST(date_time_notif AS TIMESTAMP) IS NULL

-- COMMAND ----------

SELECT * FROM event_log(table(kpn_silver.customerservice.alarm))

-- COMMAND ----------

select distinct to_timestamp(DATE_TIME,'M/dd/yyyy hh:mm a') from kpn_bronze.customerservice.alarm

-- COMMAND ----------

select * from kpn_silver.customerservice.rejection_alarm

-- COMMAND ----------

select * from kpn_bronze.customerservice.alarm
where ID_ALARM = 4330506147157166408 
and NAME_SUB_SUPPLIER = 'IOL'

-- COMMAND ----------

select * from kpn_bronze.customerservice.alarm
where ID_ALARM = -2358580416833791688
and NAME_SUB_SUPPLIER = 'UHJ'

-- COMMAND ----------

select DATE_TIME_NOTIF RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}'),
to_timestamp(DATE_TIME,'MM/dd/yyyy hh:mm a')
from kpn_bronze.customerservice.alarm
 where ID_ALARM = 4330506147157166408 
and NAME_SUB_SUPPLIER = 'IOL'

-- COMMAND ----------

ALTER STREAMING TABLE  kpn_bronze.customerservice.alarm
{
alter crtd_dttm = '2024-02-12T11:32:39.535+00:00'
where ID_ALARM = -5351972922084485355
}

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #CableModem

-- COMMAND ----------

select count(*) from kpn_bronze.network.cablemodem

-- COMMAND ----------

select count(*) from kpn_silver.network.cablemodem

-- COMMAND ----------

select * from kpn_bronze.network.cablemodem

-- COMMAND ----------

select * from kpn_silver.network.cablemodem

-- COMMAND ----------

select SNR_DN,cast(SNR_DN as decimal(15,10)) from kpn_silver.network.cablemodem

-- COMMAND ----------

select distinct(hub,mac_addr,downstream_name) from kpn_bronze.network.cablemodem

-- COMMAND ----------

select * from kpn_bronze.network.cablemodem
where hub = '6RTL-Red_Tower'
and mac_addr = "20:F1:9E:4F:8E:61"
and  downstream_name = "SWCMT0000161/00015C922072/cable-downstream 8/4/3"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Inhome

-- COMMAND ----------

select count(*) from kpn_bronze.network.inhome

-- COMMAND ----------

select count(*) from kpn_silver.network.inhome

-- COMMAND ----------

select count(*) from kpn_silver.network.rejection_inhome

-- COMMAND ----------

select * from kpn_bronze.network.inhome
where LINEID RLIKE ('^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]') 
    AND (MAC != ' ') 
    AND (STATIONTYPE RLIKE '[^\\d.-]') 
    AND (LATENCYAVERAGE < 10)
    AND (THROUGHPUTACTIVEAVERAGE < 550000)

-- COMMAND ----------

22:35:E5:44:04:5V

[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]

-- COMMAND ----------

select * from kpn_bronze.network.inhome
where LINEID NOT RLIKE ('^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]') 
    OR (MAC = ' ') 
    OR (STATIONTYPE NOT RLIKE '[^\\d.-]') 
    OR (LATENCYAVERAGE > 10)
    OR (THROUGHPUTACTIVEAVERAGE > 550000)

-- COMMAND ----------

select * from kpn_silver.network.inhome

-- COMMAND ----------

select * from kpn_silver.network.rejection_inhome

-- COMMAND ----------

select distinct(STATIONTYPE) from kpn_bronze.network.inhome

-- COMMAND ----------

select * from kpn_test.default.results
where CRTD_DTTM > 2024-03-08T
order by CRTD_DTTM desc

-- COMMAND ----------

select * from kpn_silver.network.inhome
where MAC not RLIKE '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$'