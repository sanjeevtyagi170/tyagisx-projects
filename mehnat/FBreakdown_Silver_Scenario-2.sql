-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##Silver Layer

-- COMMAND ----------

SET spark.sql.legacy.timeParserPolicy = LEGACY

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE breakdown
(
  CONSTRAINT valid_observation EXPECT(OBSERVATION != ' ') ON VIOLATION DROP ROW
)
COMMENT "Append only data with 1 data quality checks"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  CAST(ID_BREAKDOWN AS BIGINT),
  COD_AC,
  COD_PT,
  CAST(TOTAL_LR AS BIGINT),
  CAST(TOTAL_BREAKDOWN AS BIGINT),
  to_timestamp(DATE_CREATION ,'MM/dd/yyyy hh:mm a') AS DATE_CREATION,
  to_timestamp(DATE_CONFIRMATION ,'MM/dd/yyyy hh:mm a') AS DATE_CONFIRMATION,
  to_timestamp(DATE_FETCH ,'MM/dd/yyyy hh:mm a') AS DATE_FETCH,
  COD_GR_ELEM,
  CAST(PARQUE AS BIGINT),
  to_timestamp(DATE_FORECAST ,'MM/dd/yyyy hh:mm a') AS DATE_FORECAST,
  to_timestamp(DATE_FORECAST_END ,'MM/dd/yyyy hh:mm a') AS DATE_FORECAST_END,
  OBSERVATION,
  REFERENCE,
  REFERENCE_INDISP,
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM STREAM(kpn_bronze.customerservice.breakdown)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE rejection_breakdown
(
  CONSTRAINT valid_observation EXPECT(OBSERVATION = ' ') ON VIOLATION DROP ROW
)
COMMENT "Append only data with 1 data quality checks"
TBLPROPERTIES ("quality" = "silver")
AS 
SELECT 
  CAST(ID_BREAKDOWN AS BIGINT) ,
  COD_AC,
  COD_PT,
  CAST(TOTAL_LR AS BIGINT),
  CAST(TOTAL_BREAKDOWN AS BIGINT),
  to_timestamp(DATE_CREATION ,'MM/dd/yyyy hh:mm a') AS DATE_CREATION,
  to_timestamp(DATE_CONFIRMATION ,'MM/dd/yyyy hh:mm a') AS DATE_CONFIRMATION,
  to_timestamp(DATE_FETCH ,'MM/dd/yyyy hh:mm a') AS DATE_FETCH,
  COD_GR_ELEM,
  CAST(PARQUE AS BIGINT),
  to_timestamp(DATE_FORECAST ,'MM/dd/yyyy hh:mm a') AS DATE_FORECAST,
  to_timestamp(DATE_FORECAST_END ,'MM/dd/yyyy hh:mm a') AS DATE_FORECAST_END,
  OBSERVATION,
  REFERENCE,
  REFERENCE_INDISP,
  current_timestamp() AS Rejection_DTTM,
  'data_engineer_group' AS Rejected_BY
FROM STREAM(kpn_bronze.customerservice.breakdown)