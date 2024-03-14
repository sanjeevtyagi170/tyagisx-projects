-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Silver Layer

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE Curation_DownStream_silver
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
FROM STREAM(kpn_bronze.kpn_bronze_db.downstream_cable_modem_kpiss)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##Gold Layer

-- COMMAND ----------

-- CREATE OR REFRESH LIVE TABLE Curation_DownStream_gold
-- AS 
-- SELECT *, "x" as aggregation_logic
-- FROM STREAM(LIVE.Curation_DownStream_silver)