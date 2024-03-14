-- Databricks notebook source
-- MAGIC %python
-- MAGIC rules = {"valid_date" : "metrics_date > '2021-01-01'"}
-- MAGIC quarantine_rules = "NOT({0})".format(" AND ".join(rules.values()))
-- MAGIC print(quarantine_rules)

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE quarantine
(
  CONSTRAINT quaratine EXPECT (NOT(id > "2021-01-01")) ON VIOLATION DROP ROW
)
AS SELECT *
FROM STREAM(kpn_test.bronze.dslam_raw)