-- Databricks notebook source
--Curation_Inhome
CREATE OR REFRESH STREAMING LIVE TABLE Curation_Inhome
AS
SELECT 
  lineid,
  deviceid,
  mac,
  stationtype,
  latencyaverage,
  throughputactiveaverage,
  rssihistogramcount,
  rssihistogramavgtput,
  rssi,
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM bronze.default.test_topic

-- COMMAND ----------

--Curation_DownStream
CREATE OR REFRESH STREAMING LIVE TABLE Curation_DownStream
AS
SELECT 
  hub,
  mac_addr,
  to_timestamp(metrics_date,'yyyy/MM/dd HH:mm') AS metrics_date,
  downstream_name,
  CAST(cer_dn AS BIGINT),
  CAST(ccer_dn AS BIGINT),
  CAST(snr_dn AS BIGINT),
  CAST(rx_power_dn AS BIGINT),
  current_timestamp() AS CRTD_DTTM ,
  'data_engineer_group' AS CRTD_BY
FROM bronze.default.test_topic

-- COMMAND ----------

--Curation_UpStream
CREATE OR REFRESH STREAMING LIVE TABLE Curation_UpStream
AS
SELECT 
  hub,
  mac_addr,
  to_timestamp(metrics_date,'yyyy/MM/dd HH:mm') AS metrics_date,
  upstream_name,
  CAST(cer_dn AS BIGINT),
  CAST(ccer_dn AS BIGINT),
  CAST(snr_dn AS BIGINT),
  CAST(rx_power_dn AS BIGINT),
  current_timestamp() AS CRTD_DTTM ,
  'data_engineer_group' AS CRTD_BY
FROM bronze.default.test_topic

-- COMMAND ----------

--Curation_Alarm
CREATE OR REFRESH STREAMING LIVE TABLE Curation_Alarm
AS
SELECT 
  CAST(id_alarme AS BIGINT),
  to_timestamp(data_hora_notif,'yyyy/MM/dd HH:mm') AS data_hora_notif,
  to_timestamp(data_hora,'yyyy/MM/dd HH:mm') AS data_hora,
  nome_sub_sup,
  gravidade,
  tipo_notif,
  cod_local,
  nome_tecnologia,
  CAST(ins_sub_sup AS BIGINT),
  objecto,
  CAST(desc_alm AS BIGINT),
  CAST(id_notif AS BIGINT),
  current_timestamp() AS CRTD_DTTM ,
  'data_engineer_group' AS CRTD_BY
FROM bronze.default.test_topic

-- COMMAND ----------

--Curation_Breakdown
CREATE OR REFRESH STREAMING LIVE TABLE Curation_Breakdown
AS
SELECT 
  CAST(ID_AVARIA AS BIGINT),
  COD_AC,
  COD_PT,
  CAST(TOTAL_LR AS BIGINT),
  CAST(TOTAL_AVARIA AS BIGINT),
  to_timestamp(DATA_CRIACAO ,'yyyy/MM/dd HH:mm') AS DATA_CRIACAO,
  to_timestamp(DATA_CONFIRMACAO ,'yyyy/MM/dd HH:mm') AS DATA_CONFIRMACAO
  to_timestamp(DATA_FECHO ,'yyyy/MM/dd HH:mm') AS DATA_FECHO
  COD_GR_ELEM
  CAST(PARQUE AS BIGINT),
  to_timestamp(DATA_PREVISAO_INI ,'yyyy/MM/dd HH:mm') AS DATA_PREVISAO_INI
  to_timestamp(DATA_PREVISAO_FIM ,'yyyy/MM/dd HH:mm') AS DATA_PREVISAO_FIM
  OBSERVACOES
  REFERENCIA
  REFERENCIA_INDISP
  current_timestamp() AS CRTD_DTTM ,
  'data_engineer_group' AS CRTD_BY
FROM bronze.default.test_topic

-- COMMAND ----------

--Curation_Topology
CREATE OR REFRESH STREAMING LIVE TABLE Curation_Topology
AS
SELECT 
  CAST(NUM_HSI AS BIGINT),
  COD_AC,
  COD_GR_ELEM,
  DESIGNACAO,
  COD_PT,
  current_timestamp() AS CRTD_DTTM ,
  'data_engineer_group' AS CRTD_BY
FROM bronze.default.test_topic