-- Databricks notebook source
---- Marking records for processing ----
UPDATE kpn_silver.customerservice.rejection_breakdown_dt
SET DQ_STATUS = "RT"
WHERE DQ_STATUS="O";

---- Update/correct values based on DQ Rules ----
UPDATE kpn_silver.customerservice.rejection_breakdown_dt
SET OBSERVATION = "Causa Não Terminal-Fora de Serviço"
WHERE DQ_STATUS="RT";

-- DQ RULES --
-- 1. AND (OBSERVATION NOT RLIKE '^[^\d]+$'
-- 2. OR OBSERVATION = '')

---- Insert the correct records into the Good Table ----
INSERT INTO kpn_silver.customerservice.breakdown_dt (
  ID_BREAKDOWN,
  COD_AC,
  COD_PT,
  TOTAL_LR,
  TOTAL_BREAKDOWN,
  DATE_CREATION,
  DATE_CONFIRMATION,
  DATE_FETCH,
  COD_GR_ELEM,
  PARQUE,
  DATE_FORECAST,
  DATE_FORECAST_END,
  OBSERVATION,
  REFERENCE,
  REFERENCE_INDISP,
  CRTD_DTTM, 
  CRTD_BY
)
SELECT 
  ID_BREAKDOWN,
  COD_AC,
  COD_PT,
  TOTAL_LR,
  TOTAL_BREAKDOWN,
  DATE_CREATION,
  DATE_CONFIRMATION,
  DATE_FETCH,
  COD_GR_ELEM,
  PARQUE,
  DATE_FORECAST,
  DATE_FORECAST_END,
  OBSERVATION,
  REFERENCE,
  REFERENCE_INDISP,
  CRTD_DTTM,
  CRTD_BY  
FROM kpn_silver.customerservice.rejection_breakdown_dt
WHERE DQ_STATUS="RT";

---- Reset DQ_STATUS after processing ----
UPDATE kpn_silver.customerservice.rejection_breakdown_dt
SET DQ_STATUS = "R"
WHERE DQ_STATUS="RT";

-- COMMAND ----------

