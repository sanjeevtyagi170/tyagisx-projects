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

-- COMMAND ----------

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