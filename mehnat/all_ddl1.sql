---------------------------------------Bronze---------------------------------------

CREATE TABLE IF NOT EXISTS kpn_bronze.customerservice.alarm(
    created_at timestamp,
    ID_ALARM bigint, 
    DATE_TIME_NOTIF string,
    DATE_TIME string,
    NAME_SUB_SUPPLIER string,
    GRAVITY string,
    TIPO_NOTIF string,
    COD_LOCAL string,
    TECHNOLOGY_NAME string,
    INS_SUB_SUP bigint,
    OBJECT string,
    DESC_ALM string,
    ID_NOTIF bigint,
    MAC_ID string
)

CREATE TABLE IF NOT EXISTS kpn_bronze.customerservice.breakdown(
    created_at timestamp,
    ID_BREAKDOWN bigint,
    COD_AC string,
    COD_PT string,
    TOTAL_LR bigint,
    TOTAL_BREAKDOWN bigint,
    DATE_CREATION string,
    DATE_CONFIRMATION string,
    DATE_FETCH string,
    COD_GR_ELEM string,
    PARQUE bigint,
    DATE_FORECAST string,
    DATE_FORECAST_END string,
    OBSERVATION string,
    REFERENCE string,
    REFERENCE_INDISP string
)

create table IF NOT EXISTS kpn_bronze.network.cablemodem(
    created_at timestamp,
    CCER_DN bigint,
    CER_DN bigint,
    DOWNSTREAM_NAME string,
    HUB string,
    MAC_ADDR string,
    METRICS_DATE string,
    RX_POWER_DN float,
    SNR_DN float
)

create table IF NOT EXISTS kpn_bronze.network.inhome(
    created_at timestamp,
    DEVICEID string,
    LATENCYAVERAGE float,
    LINEID string,
    MAC string,
    RSSI int,
    RSSIHISTOGRAMAVGTPUT string,
    RSSIHISTOGRAMCOUNT string,
    STATIONTYPE string,
    THROUGHPUTACTIVEAVERAGE bigint
)


create table IF NOT EXISTS kpn_bronze.network.topology(
    NUM_HSI string,
    COD_AC string,
    COD_GR_ELEM string,
    DESIGNATION string,
    COD_PT string
)


-----------------------------------------Silver-------------------------------------------

-------INHOME-------
CREATE TABLE IF NOT EXISTS kpn_silver.network.inhome_dt(
    INHOME_ID BIGINT GENERATED ALWAYS AS IDENTITY,
    LINEID string,
    DEVICEID string,
    MAC string,
    STATIONTYPE string,
    LATENCYAVERAGE float,
    THROUGHPUTACTIVEAVERAGE bigint,
    RSSIHISTOGRAMCOUNT string,
    RSSIHISTOGRAMAVGTPUT string,
    RSSI int,
    CRTD_DTTM timestamp, 
    CRTD_BY string
    )

CREATE TABLE IF NOT EXISTS kpn_silver.network.rejection_inhome_dt(
    INHOME_ID BIGINT GENERATED ALWAYS AS IDENTITY,
    LINEID string,
    DEVICEID string,
    MAC string,
    STATIONTYPE string,
    LATENCYAVERAGE float,
    THROUGHPUTACTIVEAVERAGE bigint,
    RSSIHISTOGRAMCOUNT string,
    RSSIHISTOGRAMAVGTPUT string,
    RSSI int,
    CRTD_DTTM timestamp, 
    CRTD_BY string,
    DQ_STATUS string
    )


-------ALARM---------
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

-------CABLEMODEM-----------
CREATE TABLE IF NOT EXISTS kpn_silver.network.cablemodem_dt(
    cable_modem_id BIGINT GENERATED ALWAYS AS IDENTITY,
    CCER_DN bigint,
    CER_DN bigint,
    DOWNSTREAM_NAME string,
    HUB string,
    MAC_ADDR string,
    METRICS_DATE timestamp,
    RX_POWER_DN float,
    SNR_DN float,
    CRTD_DTTM timestamp, 
    CRTD_BY string
)

CREATE TABLE IF NOT EXISTS kpn_silver.network.rejection_cablemodem_dt(
    cable_modem_id BIGINT GENERATED ALWAYS AS IDENTITY,
    CCER_DN bigint,
    CER_DN bigint,
    DOWNSTREAM_NAME string,
    HUB string,
    MAC_ADDR string,
    METRICS_DATE timestamp,
    RX_POWER_DN float,
    SNR_DN float,
    CRTD_DTTM timestamp, 
    CRTD_BY string,
    DQ_STATUS string
)

---------------BREAKDOWN---------------
-- Databricks notebook source
CREATE TABLE kpn_silver.customerservice.breakdown_dt(
    breakdown_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ID_BREAKDOWN bigint,
    COD_AC string,
    COD_PT string,
    TOTAL_LR bigint,
    TOTAL_BREAKDOWN bigint,
    DATE_CREATION timestamp,
    DATE_CONFIRMATION timestamp,
    DATE_FETCH timestamp,
    COD_GR_ELEM string,
    PARQUE bigint,
    DATE_FORECAST timestamp,
    DATE_FORECAST_END timestamp,
    OBSERVATION string,
    REFERENCE string,
    REFERENCE_INDISP string,
    CRTD_DTTM timestamp, 
    CRTD_BY string
)

CREATE TABLE kpn_silver.customerservice.rejection_breakdown_dt(
    breakdown_id BIGINT GENERATED ALWAYS AS IDENTITY,
    ID_BREAKDOWN bigint,
    COD_AC string,
    COD_PT string,
    TOTAL_LR bigint,
    TOTAL_BREAKDOWN bigint,
    DATE_CREATION timestamp,
    DATE_CONFIRMATION timestamp,
    DATE_FETCH timestamp,
    COD_GR_ELEM string,
    PARQUE bigint,
    DATE_FORECAST timestamp,
    DATE_FORECAST_END timestamp,
    OBSERVATION string,
    REFERENCE string,
    REFERENCE_INDISP string,
    CRTD_DTTM timestamp, 
    CRTD_BY string,
    DQ_STATUS string
)

