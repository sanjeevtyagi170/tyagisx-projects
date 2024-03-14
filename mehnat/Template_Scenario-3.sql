-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Silver Layer

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kpn_silver.kpn_silver_db.curation_downstream_silver_non_dlt
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
FROM kpn_bronze.kpn_bronze_db.downstream_cable_modem_kpiss

-- COMMAND ----------

MERGE INTO kpn_silver.kpn_silver_db.curation_downstream_silver_non_dlt AS t 
USING kpn_bronze.kpn_bronze_db.downstream_cable_modem_kpiss AS s
ON t.hub = s.hub
AND t.mac_addr = s.mac_addr
AND t.downstream_name = s.downstream_name
WHEN NOT MATCHED THEN
INSERT(hub,mac_addr,metrics_date,downstream_name,cer_dn,ccer_dn,snr_dn,rx_power_dn,CRTD_DTTM,CRTD_BY)
VALUES(s.hub,s.mac_addr,to_timestamp(s.metrics_date,'yyyy/MM/dd HH:mm'),s.downstream_name,s.cer_dn,s.ccer_dn,s.snr_dn,s.rx_power_dn,current_timestamp(),'data_engineer_group')

-- COMMAND ----------

-- INSERT INTO kpn_test.default.curation_downstream_bronze
-- VALUES('2STL-St_22Leonards','576CB17B1D5B645','2022/01/01 00:00','SWCMT0000161/00015C922072/cable-downstream 8/4/3', 1401188196,937410221,0.9298737,0.8371177,null),
-- ('6RTL-Red_Tower','448D18C2E6C63','2022/01/01 00:00','SWCMT0000158/00015C92206F/cable-downstream 5/1/0',1671095145,1511172700,0.3276527,0.4438011,null)

-- COMMAND ----------

-- ALTER TABLE kpn_silver.kpn_silver_db.curation_downstream_silver_non_dlt ALTER COLUMN hub SET NOT NULL;
-- ALTER TABLE kpn_silver.kpn_silver_db.curation_downstream_silver_non_dlt ALTER COLUMN mac_addr SET NOT NULL;
-- ALTER TABLE kpn_silver.kpn_silver_db.curation_downstream_silver_non_dlt ALTER COLUMN downstream_name SET NOT NULL;
-- ALTER TABLE kpn_silver.kpn_silver_db.curation_downstream_silver_non_dlt ADD CONSTRAINT test_pk PRIMARY KEY(hub,mac_addr,downstream_name);