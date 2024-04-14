-- Databricks notebook source
---- Marking records for processing ----
UPDATE kpn_silver.customerservice.rejection_alarm_dt
SET DQ_STATUS = "RT"
WHERE DQ_STATUS="O";

---- Update/correct values based on DQ Rules ----
UPDATE kpn_silver.customerservice.rejection_alarm_dt
SET DATE_TIME_NOTIF = CAST("2024-02-25 13:59:59" as timestamp), TIPO_NOTIF = "H"
WHERE DQ_STATUS="RT";

-- DQ RULES --
-- 1. AND ((CAST(DATE_TIME_NOTIF AS TIMESTAMP) IS NULL 
-- 2. OR TIPO_NOTIF NOT RLIKE '^[A-Z]{1}$'))

---- Insert the correct records into the Good Table ----
INSERT INTO kpn_silver.customerservice.alarm_dt (
  ID_ALARM, 
  DATE_TIME_NOTIF,
  DATE_TIME,
  NAME_SUB_SUPPLIER ,
  GRAVITY ,
  TIPO_NOTIF ,
  COD_LOCAL ,
  TECHNOLOGY_NAME ,
  INS_SUB_SUP,
  OBJECT,
  DESC_ALM,
  ID_NOTIF,
  MAC_ID,
  CRTD_DTTM , 
  CRTD_BY
  )
SELECT 
  ID_ALARM,
  DATE_TIME_NOTIF,
  DATE_TIME,
  NAME_SUB_SUPPLIER,
  GRAVITY,
  TIPO_NOTIF,
  COD_LOCAL,
  TECHNOLOGY_NAME,
  INS_SUB_SUP,
  OBJECT,
  DESC_ALM,
  ID_NOTIF,
  MAC_ID,
  CRTD_DTTM,
  CRTD_BY  
FROM kpn_silver.customerservice.rejection_alarm_dt
WHERE DQ_STATUS="RT";

---- Reset DQ_STATUS after processing ----
UPDATE kpn_silver.customerservice.rejection_alarm_dt
SET DQ_STATUS = "R"
WHERE DQ_STATUS="RT";