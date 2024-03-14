-- Databricks notebook source
--STEP 1--
UPDATE kpn_bronze.customerservice.breakdown_dt
SET wm_flag = "NT"
WHERE wm_flag="N";

-- COMMAND ----------

SET spark.sql.legacy.timeParserPolicy = LEGACY ;
--STEP 2--
---GOOD TABLE---
INSERT INTO kpn_silver.customerservice.breakdown_dt(
  ID_BREAKDOWN,
  COD_AC,
  COD_PT,
  TOTAL_LR,
  TOTAL_BREAKDOWN,
  DATE_CREATION,
  DATE_CONFIRMATION,
  DATE_FETCH,
  COD_GR_ELEM,
  PARQUE,
  DATE_FORECAST,
  DATE_FORECAST_END,
  OBSERVATION,
  REFERENCE,
  REFERENCE_INDISP,
  CRTD_DTTM, 
  CRTD_BY
)
SELECT 
  CAST(ID_BREAKDOWN AS BIGINT),
  COD_AC,
  COD_PT,
  CAST(TOTAL_LR AS BIGINT),
  CAST(TOTAL_BREAKDOWN AS BIGINT),
  to_timestamp(DATE_CREATION ,'yyyy-MM-dd HH:mm:ss') AS DATE_CREATION,
  to_timestamp(DATE_CONFIRMATION ,'yyyy-MM-dd HH:mm:ss') AS DATE_CONFIRMATION,
  to_timestamp(DATE_FETCH ,'yyyy-MM-dd HH:mm:ss') AS DATE_FETCH,
  COD_GR_ELEM,
  CAST(PARQUE AS BIGINT),
  to_timestamp(DATE_FORECAST ,'yyyy-MM-dd HH:mm:ss') AS DATE_FORECAST,
  to_timestamp(DATE_FORECAST_END ,'yyyy-MM-dd HH:mm:ss') AS DATE_FORECAST_END,
  OBSERVATION,
  REFERENCE,
  REFERENCE_INDISP,
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM kpn_bronze.customerservice.breakdown_dt
WHERE wm_flag="NT" AND OBSERVATION != ''


-- COMMAND ----------

---BAD TABLE---
INSERT INTO kpn_silver.customerservice.rejection_breakdown_dt (
  ID_BREAKDOWN,
  COD_AC,
  COD_PT,
  TOTAL_LR,
  TOTAL_BREAKDOWN,
  DATE_CREATION,
  DATE_CONFIRMATION,
  DATE_FETCH,
  COD_GR_ELEM,
  PARQUE,
  DATE_FORECAST,
  DATE_FORECAST_END,
  OBSERVATION,
  REFERENCE,
  REFERENCE_INDISP,
  DQ_STATUS,
  CRTD_DTTM, 
  CRTD_BY
)
SELECT 
  CAST(ID_BREAKDOWN AS BIGINT),
  COD_AC,
  COD_PT,
  CAST(TOTAL_LR AS BIGINT),
  CAST(TOTAL_BREAKDOWN AS BIGINT),
  to_timestamp(DATE_CREATION ,'yyyy-MM-dd HH:mm:ss') AS DATE_CREATION,
  to_timestamp(DATE_CONFIRMATION ,'yyyy-MM-dd HH:mm:ss') AS DATE_CONFIRMATION,
  to_timestamp(DATE_FETCH ,'yyyy-MM-dd HH:mm:ss') AS DATE_FETCH,
  COD_GR_ELEM,
  CAST(PARQUE AS BIGINT),
  to_timestamp(DATE_FORECAST ,'yyyy-MM-dd HH:mm:ss') AS DATE_FORECAST,
  to_timestamp(DATE_FORECAST_END ,'yyyy-MM-dd HH:mm:ss') AS DATE_FORECAST_END,
  OBSERVATION,
  REFERENCE,
  REFERENCE_INDISP,
  'O' as DQ_STATUS,
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM kpn_bronze.customerservice.breakdown_dt
WHERE wm_flag="NT" AND OBSERVATION = ''

-- COMMAND ----------

--STEP 3--
UPDATE kpn_bronze.customerservice.breakdown_dt
SET wm_flag = "Y"
WHERE wm_flag="NT";