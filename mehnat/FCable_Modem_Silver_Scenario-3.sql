-- Databricks notebook source
CREATE TABLE IF NOT EXISTS kpn_silver.network.cablemodem
AS
SELECT 
  HUB,
  MAC_ADDR,
  to_timestamp(METRICS_DATE,'yyyy/MM/dd HH:mm') AS METRICS_DATE,
  DOWNSTREAM_NAME,
  CAST(CER_DN AS BIGINT),
  CAST(CCER_DN AS BIGINT),
  SNR_DN,
  RX_POWER_DN,
  current_timestamp() AS CRTD_DTTM ,
  'data_engineer_group' AS CRTD_BY
FROM kpn_bronze.network.cablemodem
WHERE METRICS_DATE RLIKE ('^[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}')
AND MAC_ADDR != ' '

-- COMMAND ----------

MERGE INTO kpn_silver.network.cablemodem AS t 
USING kpn_bronze.network.cablemodem AS s
ON t.HUB = s.HUB
AND t.MAC_ADDR = s.MAC_ADDR
AND t.METRICS_DATE = to_timestamp(s.METRICS_DATE,'yyyy/MM/dd HH:mm') 
AND t.DOWNSTREAM_NAME = s.DOWNSTREAM_NAME
AND t.CER_DN = s.CER_DN 
AND t.CCER_DN = s.CCER_DN
AND t.SNR_DN = s.SNR_DN 
AND t.RX_POWER_DN = s.RX_POWER_DN
WHEN NOT MATCHED AND s.metrics_date RLIKE ('^[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}') AND MAC_ADDR != ' ' THEN
INSERT(hub,mac_addr,metrics_date,downstream_name,cer_dn,ccer_dn,snr_dn,rx_power_dn,CRTD_DTTM,CRTD_BY)
VALUES(s.hub,s.mac_addr,to_timestamp(s.metrics_date,'yyyy/MM/dd HH:mm'),s.downstream_name,s.cer_dn,s.ccer_dn,s.snr_dn,s.rx_power_dn,current_timestamp(),'data_engineer_group')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kpn_silver.network.rejection_cablemodem
AS
SELECT 
  HUB,
  MAC_ADDR,
  METRICS_DATE,
  DOWNSTREAM_NAME,
  CAST(CER_DN AS BIGINT),
  CAST(CCER_DN AS BIGINT),
  SNR_DN,
  RX_POWER_DN,
  current_timestamp() AS Rejected_DTTM ,
  'data_engineer_group' AS Rejected_BY
FROM kpn_bronze.network.cablemodem
-- WHERE METRICS_DATE NOT RLIKE ('^[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}') 
-- OR MAC_ADDR = ' '

-- COMMAND ----------

MERGE INTO kpn_silver.network.rejection_cablemodem AS t 
USING kpn_bronze.network.cablemodem AS s
ON t.HUB = s.HUB
AND t.MAC_ADDR = s.MAC_ADDR
AND t.METRICS_DATE = s.METRICS_DATE
AND t.DOWNSTREAM_NAME = s.DOWNSTREAM_NAME
AND t.CER_DN = s.CER_DN 
AND t.CCER_DN = s.CCER_DN
AND t.SNR_DN = s.SNR_DN 
AND t.RX_POWER_DN = s.RX_POWER_DN
WHEN NOT MATCHED AND (s.metrics_date NOT RLIKE ('^[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}') OR MAC_ADDR = ' ')THEN
INSERT(hub,mac_addr,metrics_date,downstream_name,cer_dn,ccer_dn,snr_dn,rx_power_dn,Rejected_DTTM,Rejected_BY)
VALUES(s.hub,s.mac_addr,s.metrics_date,s.downstream_name,s.cer_dn,s.ccer_dn,s.snr_dn,s.rx_power_dn,current_timestamp(),'data_engineer_group')