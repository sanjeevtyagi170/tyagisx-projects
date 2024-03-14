-- Databricks notebook source
-- COPY INTO
CREATE TABLE IF NOT EXISTS kpn_bronze.network.topology;

COPY INTO kpn_bronze.network.topology
FROM "s3://uc-kpn-landing-bucket/batch/Topology-*.csv"
FILEFORMAT = CSV
FORMAT_OPTIONS ('mergeSchema' = 'true','header' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');


-- COMMAND ----------

CREATE or replace table kpn_silver.network.topology
AS
SELECT
  CAST(NUM_HSI AS BIGINT),
  COD_AC,
  COD_GR_ELEM,
  DESIGNATION,
  COD_PT,
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM kpn_bronze.network.topology
WHERE COD_GR_ELEM RLIKE '[^\\d.-]' AND LEN(COD_GR_ELEM) <= 12

-- COMMAND ----------

MERGE INTO kpn_silver.network.topology AS t 
USING kpn_bronze.network.topology AS s
ON t.NUM_HSI = CAST(s.NUM_HSI AS BIGINT)
--AND t.COD_AC = s.COD_AC
AND t.COD_GR_ELEM = s.COD_GR_ELEM
AND t.DESIGNATION = s.DESIGNATION
AND t.COD_PT = s.COD_PT
WHEN NOT MATCHED AND COD_GR_ELEM RLIKE '[^\\d.-]' AND LEN(COD_GR_ELEM) <= 12 THEN
INSERT(NUM_HSI,COD_AC,COD_GR_ELEM,DESIGNATION,COD_PT,CRTD_DTTM,CRTD_BY)
VALUES(CAST(s.NUM_HSI AS BIGINT),s.COD_AC,s.COD_GR_ELEM,s.DESIGNATION,s.COD_PT,current_timestamp(),'data_engineer_group')

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kpn_silver.network.rejection_topology
AS
SELECT
  CAST(NUM_HSI AS BIGINT),
  COD_AC,
  COD_GR_ELEM,
  DESIGNATION,
  COD_PT,
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM kpn_bronze.network.topology
WHERE COD_GR_ELEM NOT RLIKE '[^\\d.-]' OR LEN(COD_GR_ELEM) > 12

-- COMMAND ----------

MERGE INTO kpn_silver.network.rejection_topology AS t 
USING kpn_bronze.network.topology AS s
ON t.NUM_HSI = CAST(s.NUM_HSI AS BIGINT)
--AND t.COD_AC = s.COD_AC
AND t.COD_GR_ELEM = s.COD_GR_ELEM
AND t.DESIGNATION = s.DESIGNATION
AND t.COD_PT = s.COD_PT
WHEN NOT MATCHED AND COD_GR_ELEM NOT RLIKE '[^\\d.-]' AND LEN(COD_GR_ELEM) > 12 THEN
INSERT(NUM_HSI,COD_AC,COD_GR_ELEM,DESIGNATION,COD_PT,CRTD_DTTM,CRTD_BY)
VALUES(CAST(s.NUM_HSI AS BIGINT),s.COD_AC,s.COD_GR_ELEM,s.DESIGNATION,s.COD_PT,current_timestamp(),'data_engineer_group')