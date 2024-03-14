-- Databricks notebook source
CREATE TABLE kpn_silver.customerservice.alarm_dt(
    alarm_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ID_ALARM bigint, 
    DATE_TIME_NOTIF timestamp,
    DATE_TIME timestamp,
    NAME_SUB_SUPPLIER string,
    GRAVITY string,
    TIPO_NOTIF string,
    COD_LOCAL string,
    TECHNOLOGY_NAME string,
    INS_SUB_SUP bigint,
    OBJECT string,
    DESC_ALM string,
    ID_NOTIF bigint,
    MAC_ID string,
    CRTD_DTTM timestamp, 
    CRTD_BY string
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kpn_silver.customerservice.rejection_alarm_dt(
    alarm_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ID_ALARM bigint, 
    DATE_TIME_NOTIF timestamp,
    DATE_TIME timestamp,
    NAME_SUB_SUPPLIER string,
    GRAVITY string,
    TIPO_NOTIF string,
    COD_LOCAL string,
    TECHNOLOGY_NAME string,
    INS_SUB_SUP bigint,
    OBJECT string,
    DESC_ALM string,
    ID_NOTIF bigint,
    MAC_ID string,
    CRTD_DTTM timestamp, 
    CRTD_BY string,
    DQ_STATUS string
    )