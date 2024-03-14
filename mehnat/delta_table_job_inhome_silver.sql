-- Databricks notebook source
--STEP 1--
UPDATE kpn_bronze.network.inhome_dt
SET wm_flag = "NT"
WHERE wm_flag="N";

-- COMMAND ----------

SET spark.sql.legacy.timeParserPolicy = LEGACY ;
--STEP 2--
---GOOD TABLE---
INSERT INTO kpn_silver.network.inhome_dt(
  LINEID,
  DEVICEID,
  MAC,
  STATIONTYPE,
  LATENCYAVERAGE,
  THROUGHPUTACTIVEAVERAGE,
  RSSIHISTOGRAMCOUNT,
  RSSIHISTOGRAMAVGTPUT,
  RSSI,
  CRTD_DTTM, 
  CRTD_BY
)
SELECT 
  LINEID,
  DEVICEID,
  MAC,
  STATIONTYPE,
  LATENCYAVERAGE,
  THROUGHPUTACTIVEAVERAGE,
  RSSIHISTOGRAMCOUNT,
  RSSIHISTOGRAMAVGTPUT,
  RSSI,
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM kpn_bronze.network.inhome_dt
WHERE wm_flag="NT"
AND (MAC RLIKE '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$') 
AND (STATIONTYPE RLIKE '[^\\d.-]') 
AND (LATENCYAVERAGE < 10)



-- COMMAND ----------

---BAD TABLE---
INSERT INTO kpn_silver.network.rejection_inhome_dt (
  LINEID,
  DEVICEID,
  MAC,
  STATIONTYPE,
  LATENCYAVERAGE,
  THROUGHPUTACTIVEAVERAGE,
  RSSIHISTOGRAMCOUNT,
  RSSIHISTOGRAMAVGTPUT,
  RSSI,
  DQ_STATUS,
  CRTD_DTTM, 
  CRTD_BY
)
SELECT 
  LINEID,
  DEVICEID,
  MAC,
  STATIONTYPE,
  LATENCYAVERAGE,
  THROUGHPUTACTIVEAVERAGE,
  RSSIHISTOGRAMCOUNT,
  RSSIHISTOGRAMAVGTPUT,
  RSSI,
  'O' as DQ_STATUS,
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM kpn_bronze.network.inhome_dt
WHERE wm_flag="NT" 
AND ((MAC NOT RLIKE '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$') 
OR (STATIONTYPE NOT RLIKE '[^\\d.-]') 
OR (LATENCYAVERAGE >= 10))


-- COMMAND ----------

--STEP 3--
UPDATE kpn_bronze.network.inhome_dt
SET wm_flag = "Y"
WHERE wm_flag="NT";

-- COMMAND ----------

