-- Databricks notebook source
---- Marking records for processing ----
UPDATE kpn_silver.network.rejection_inhome_dt
SET DQ_STATUS = "RT"
WHERE DQ_STATUS="O";

---- Update/correct values based on DQ Rules ----
UPDATE kpn_silver.network.rejection_inhome_dt
SET MAC = "31:31:D3:42:07:5E",STATIONTYPE="Huawei_Phone",LATENCYAVERAGE=0.68173003
WHERE DQ_STATUS="RT";

-- DQ RULES --
-- 1. ((MAC NOT RLIKE '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$') 
-- 2. OR (STATIONTYPE NOT RLIKE '[^\\d.-]') 
-- 3. OR (LATENCYAVERAGE >= 10))

---- Insert the correct records into the Good Table ----
INSERT INTO kpn_silver.network.inhome_dt (
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
  CRTD_DTTM,
  CRTD_BY
FROM kpn_silver.network.rejection_inhome_dt
WHERE DQ_STATUS="RT";

---- Reset DQ_STATUS after processing ----
UPDATE kpn_silver.network.rejection_inhome_dt
SET DQ_STATUS = "R"
WHERE DQ_STATUS="RT";