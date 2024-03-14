-- Databricks notebook source
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

-- COMMAND ----------

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