-- Databricks notebook source
SET spark.sql.legacy.timeParserPolicy = LEGACY

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE alarm
(
  CONSTRAINT valid_date_time_notif EXPECT(CAST(DATE_TIME_NOTIF AS TIMESTAMP) IS NOT NULL AND TIPO_NOTIF RLIKE '^[A-Z]{1}$') ON VIOLATION DROP ROW
  --CONSTRAINT valid_date_time_notif EXPECT(DATE_TIME_NOTIF RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}'))ON VIOLATION DROP ROW
)
AS
SELECT 
  ID_ALARM,
  to_timestamp(DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
  to_timestamp(DATE_TIME,'MM/dd/yyyy hh:mm a') AS DATE_TIME,
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
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM STREAM(kpn_bronze.customerservice.alarm)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE rejection_alarm
(
  CONSTRAINT valid_date_time_notif EXPECT(CAST(DATE_TIME_NOTIF AS TIMESTAMP) IS NULL AND TIPO_NOTIF NOT RLIKE '^[A-Z]{1}$') ON VIOLATION DROP ROW
  --CONSTRAINT valid_date_time_notif EXPECT(DATE_TIME_NOTIF NOT RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}')) ON VIOLATION DROP ROW,
 
)
AS
SELECT 
  ID_ALARM,
  to_timestamp(DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
  to_timestamp(DATE_TIME,'MM/dd/yyyy hh:mm a') AS DATE_TIME,
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
  current_timestamp() AS Rejection_DTTM,
  'data_engineer_group' AS Rejected_BY
FROM STREAM(kpn_bronze.customerservice.alarm)