-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE inhome
(
  CONSTRAINT valid_lineid EXPECT
  (
    (LINEID RLIKE '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$') 
    AND (MAC RLIKE '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$') 
    AND (STATIONTYPE RLIKE '[^\\d.-]') 
    AND (LATENCYAVERAGE < 10)
    AND (THROUGHPUTACTIVEAVERAGE < 550000)
  ) 
  ON VIOLATION DROP ROW
)
AS
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
FROM STREAM(kpn_bronze.network.inhome)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE rejection_inhome
(
  CONSTRAINT valid_lineid EXPECT
  (
    (LINEID NOT RLIKE '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$') 
    OR (MAC NOT RLIKE  '^[0-9]{2}:[0-9]{2}:[A-Z][0-9]:[0-9]{2}:[0-9]{2}:[0-9][A-Z]$') 
    OR (STATIONTYPE RLIKE '[0-9]') 
    OR (LATENCYAVERAGE > 10)
    OR (THROUGHPUTACTIVEAVERAGE > 550000)
  )
  ON VIOLATION DROP ROW
)
AS
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
  current_timestamp() AS Rejection_DTTM,
  'data_engineer_group' AS Rejected_BY
FROM STREAM(kpn_bronze.network.inhome)