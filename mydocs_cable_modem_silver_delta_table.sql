-- Databricks notebook source
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

-- COMMAND ----------

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
