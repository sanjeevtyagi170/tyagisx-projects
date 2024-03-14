-- Databricks notebook source
create or replace temp view breakrdwon_vw
as
select
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
WHERE --wm_flag="NT" 
OBSERVATION RLIKE '[^\\d.-]'
AND OBSERVATION != ''

-- COMMAND ----------

select * from breakrdwon_vw

-- COMMAND ----------


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
WHERE --wm_flag="NT" 
MAC_ADDR != '' 
AND METRICS_DATE RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}')