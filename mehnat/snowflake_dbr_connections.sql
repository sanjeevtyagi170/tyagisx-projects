-- Databricks notebook source
-- create database if not exists kpn_test.kpn_demo; -- catalog.table
create table if not exists kpn_demo.x(a numeric, b numeric);
select * from kpn_demo.x;
insert into kpn_demo.x values(2,1);
insert into kpn_demo.x values(3,4);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.sql("select * from kpn_demo.x")
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC options = {
-- MAGIC   "sfUrl": "hpujerb-fz70925.snowflakecomputing.com/",
-- MAGIC   "sfUser": "SAMBITSAHU",
-- MAGIC   "sfPassword": "ParrotGreen@1",
-- MAGIC   "sfDatabase": "KPN",
-- MAGIC   "sfSchema": "KPN_POC",
-- MAGIC   "sfWarehouse": "COMPUTE_WH",
-- MAGIC   "dbtable": "x"
-- MAGIC }

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.write.format("snowflake").options(**options).mode("overwrite").save()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sfUtils = sc._jvm.net.snowflake.spark.snowflake.Utils     #spark.databricks.security.py4j.whitelist

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #tabname = "select 10,6 from dual union all select 11,12 from dual"
-- MAGIC #copyintoquery = "insert into x {}".format(tabname)
-- MAGIC #copyintoquery = "call output_message1()"
-- MAGIC src = "(select 10 a,0 b from dual union all select 39,12 from dual)"
-- MAGIC copyintoquery = "MERGE INTO KPN.KPN_POC.x as x1 USING {} AS c ON x1.a = c.a WHEN MATCHED THEN UPDATE SET x1.b = c.b WHEN NOT MATCHED THEN INSERT (a, b) VALUES (c.a, c.b)".format(src)
-- MAGIC print(copyintoquery)

-- COMMAND ----------

-- %python
-- spark.conf.set("spark.databricks.pyspark.enablePy4JSecurity","false")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC queryobject = sfUtils.runQuery(options,copyintoquery)
