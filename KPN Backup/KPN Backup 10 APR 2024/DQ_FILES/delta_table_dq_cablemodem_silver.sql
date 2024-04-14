-- Databricks notebook source
---- Marking records for processing ----
UPDATE kpn_silver.network.rejection_cablemodem_dt
SET DQ_STATUS = "RT"
WHERE DQ_STATUS="O";

---- Update/correct values based on DQ Rules ----
UPDATE kpn_silver.network.rejection_cablemodem_dt
SET MAC_ADDR = "18:35:D1:44:04:5B",METRICS_DATE=CAST("2024-02-25 13:59:59" as timestamp)
WHERE DQ_STATUS="RT";
-- DQ RULES --
-- 1. ( MAC_ADDR = '' OR 
-- 2. METRICS_DATE NOT RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}'))

---- Insert the correct records into the Good Table ----
INSERT INTO kpn_silver.network.cablemodem_dt (
  HUB,
  MAC_ADDR,
  METRICS_DATE,
  DOWNSTREAM_NAME,
  CER_DN,
  CCER_DN,
  SNR_DN,
  RX_POWER_DN,
  CRTD_DTTM,
  CRTD_BY
)
SELECT 
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
FROM kpn_silver.network.rejection_cablemodem_dt
WHERE DQ_STATUS="RT";

---- Reset DQ_STATUS after processing ----
UPDATE kpn_silver.network.rejection_cablemodem_dt
SET DQ_STATUS = "R"
WHERE DQ_STATUS="RT";