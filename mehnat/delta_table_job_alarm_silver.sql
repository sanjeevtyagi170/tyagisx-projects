-- Databricks notebook source
--STEP 1--
UPDATE kpn_bronze.customerservice.alarm_dt
SET wm_flag = "NT"
WHERE wm_flag="N";

-- COMMAND ----------

SET spark.sql.legacy.timeParserPolicy = LEGACY ;
--STEP 2--
---GOOD TABLE---
INSERT INTO kpn_silver.customerservice.alarm_dt (
    ID_ALARM, 
    DATE_TIME_NOTIF,
    DATE_TIME,
    NAME_SUB_SUPPLIER ,
    GRAVITY ,
    TIPO_NOTIF ,
    COD_LOCAL ,
    TECHNOLOGY_NAME ,
    INS_SUB_SUP ,
    OBJECT ,
    DESC_ALM ,
    ID_NOTIF ,
    MAC_ID ,
    CRTD_DTTM , 
    CRTD_BY)
SELECT 
  ID_ALARM,
  to_timestamp(DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
  to_timestamp(DATE_TIME,'yyyy-MM-dd HH:mm:ss') AS DATE_TIME,
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
FROM kpn_bronze.customerservice.alarm_dt
WHERE wm_flag="NT" 
and (CAST(DATE_TIME_NOTIF AS TIMESTAMP) IS NOT NULL 
AND TIPO_NOTIF RLIKE '^[A-Z]{1}$')


-- COMMAND ----------

---BAD TABLE---
INSERT INTO kpn_silver.customerservice.rejection_alarm_dt (
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
    DQ_STATUS,
    CRTD_DTTM , 
    CRTD_BY)
SELECT 
    ID_ALARM,
    to_timestamp(DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
    to_timestamp(DATE_TIME,'yyyy-MM-dd HH:mm:ss') AS DATE_TIME,
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
    'O' as DQ_STATUS,
    current_timestamp() AS CRTD_DTTM,
    'data_engineer_group' AS CRTD_BY  
FROM kpn_bronze.customerservice.alarm_dt
WHERE wm_flag="NT" 
AND ((CAST(DATE_TIME_NOTIF AS TIMESTAMP) IS NULL 
OR TIPO_NOTIF NOT RLIKE '^[A-Z]{1}$'))

-- COMMAND ----------

--STEP 3--
UPDATE kpn_bronze.customerservice.alarm_dt
SET wm_flag = "Y"
WHERE wm_flag="NT";