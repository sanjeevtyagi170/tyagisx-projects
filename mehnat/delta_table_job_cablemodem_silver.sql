-- Databricks notebook source
--STEP 1--
UPDATE kpn_bronze.network.cablemodem_dt
SET wm_flag = "NT"
WHERE wm_flag="N";

-- COMMAND ----------

SET spark.sql.legacy.timeParserPolicy = LEGACY ;
--STEP 2--
---GOOD TABLE---
INSERT INTO kpn_silver.network.cablemodem_dt(
  HUB,
  MAC_ADDR,
  METRICS_DATE,
  DOWNSTREAM_NAME,
  CER_DN,
  CCER_DN,
  SNR_DN,
  RX_POWER_DN,
  CRTD_DTTM ,
  CRTD_BY
)
SELECT 
  HUB,
  MAC_ADDR,
  to_timestamp(METRICS_DATE,'yyyy-MM-dd HH:mm:ss') AS METRICS_DATE,
  DOWNSTREAM_NAME,
  CAST(CER_DN AS BIGINT),
  CAST(CCER_DN AS BIGINT),
  SNR_DN,
  RX_POWER_DN,
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM kpn_bronze.network.cablemodem_dt
WHERE wm_flag="NT" 
AND METRICS_DATE RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$')
AND MAC_ADDR != ' '


-- COMMAND ----------

---BAD TABLE---
INSERT INTO kpn_silver.network.rejection_cablemodem_dt (
  HUB,
  MAC_ADDR,
  METRICS_DATE,
  DOWNSTREAM_NAME,
  CER_DN,
  CCER_DN,
  SNR_DN,
  RX_POWER_DN,
  DQ_STATUS,
  CRTD_DTTM ,
  CRTD_BY
)
SELECT 
  HUB,
  MAC_ADDR,
  to_timestamp(METRICS_DATE,'yyyy-MM-dd HH:mm:ss') AS METRICS_DATE,
  DOWNSTREAM_NAME,
  CAST(CER_DN AS BIGINT),
  CAST(CCER_DN AS BIGINT),
  SNR_DN,
  RX_POWER_DN,
  'O' as DQ_STATUS,
  current_timestamp() AS CRTD_DTTM ,
  'data_engineer_group' AS CRTD_BY
FROM kpn_bronze.network.cablemodem_dt
WHERE wm_flag="NT" 
AND ( MAC_ADDR = ' ' OR 
METRICS_DATE NOT RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}'))

-- COMMAND ----------

--STEP 3--
UPDATE kpn_bronze.network.cablemodem_dt
SET wm_flag = "Y"
WHERE wm_flag="NT";

-- COMMAND ----------

SELECT 
distinct metrics_date
FROM kpn_bronze.network.cablemodem_dt
WHERE wm_flag="Y" 
AND ((METRICS_DATE NOT RLIKE ("^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}$"))
OR MAC_ADDR = ' ')