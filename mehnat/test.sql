-- Databricks notebook source
select * from kpn_bronze.customerservice.breakdown_dt

-- COMMAND ----------

select count(*) from kpn_silver.customerservice.breakdown

-- COMMAND ----------

CREATE or replace streaming live TABLE alarm_test(
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
)

-- COMMAND ----------

SELECT COUNT(*) FROM kpn_bronze.network.cablemodem

-- COMMAND ----------

select count(*) from kpn_bronze.customerservice.breakdown

-- COMMAND ----------

select count(*) from kpn_silver.network.inhome

-- COMMAND ----------

select count(*) from kpn_silver.customerservice.alarm

-- COMMAND ----------

SELECT * FROM kpn_bronze.network.kpninhome

-- COMMAND ----------

  -- select count(*) from kpn_silver.network.topology
  select * from parquet.`s3://uc-kpn-landing-bucket/customerservice/breakdown/`

-- COMMAND ----------

select count(*) from kpn_silver.kpn_silver_db.curation_topology_silver;

-- COMMAND ----------

select * from kpn_bronze.customerservice.alarm;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS alarms_db.alarms_1 (
  id INT,
  name STRING
)

-- COMMAND ----------

SELECT * FROM PARQUET.`s3://uc-kpn-landing-bucket/topics/breakdown/*/*/*/*`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #SNOWFLAKE

-- COMMAND ----------

GENERATE symlink_customer_alarm_manifest FOR TABLE kpn_silver.customerservice.alarm

-- COMMAND ----------

-- Truncate bronze
-- truncate table kpn_bronze.customerservice.alarm;
-- truncate table kpn_bronze.customerservice.breakdown;
-- truncate table kpn_bronze.network.cablemodem;
-- truncate table kpn_bronze.network.inhome;
-- truncate table kpn_bronze.network.topology;

-- COMMAND ----------

-- Truncate Silver
-- truncate table kpn_silver.customerservice.alarm;
-- truncate table kpn_silver.customerservice.breakdown;
-- truncate table kpn_silver.network.cablemodem;
-- truncate table kpn_silver.network.inhome;
-- truncate table kpn_silver.network.topology;

-- truncate table kpn_silver.customerservice.rejection_alarm;
-- truncate table kpn_silver.customerservice.rejection_breakdown;
-- truncate table kpn_silver.network.rejection_cablemodem;
-- truncate table kpn_silver.network.rejection_inhome;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import datetime
-- MAGIC import random
-- MAGIC
-- MAGIC def generate_random_datetime():
-- MAGIC   min_year = 2024
-- MAGIC   max_year = datetime.datetime.today().year
-- MAGIC   min_month = 1
-- MAGIC   max_month = datetime.datetime.today().month
-- MAGIC   min_day = 1
-- MAGIC   max_day = datetime.datetime.today().day
-- MAGIC   
-- MAGIC   year = random.randint(min_year, max_year)
-- MAGIC   month = random.randint(min_month,max_month)
-- MAGIC   day = random.randint(min_day, max_day)
-- MAGIC   hour = random.randint(0, 23)
-- MAGIC   minute = random.randint(0, 59)
-- MAGIC   second = random.randint(0, 59)
-- MAGIC   microsecond = random.randint(0, 999999)
-- MAGIC   random_datetime = datetime.datetime(year, month, day, hour, minute, second, microsecond)
-- MAGIC   formatted_datetime = random_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
-- MAGIC
-- MAGIC   return formatted_datetime
-- MAGIC random_datetime_str = generate_random_datetime()
-- MAGIC print(random_datetime_str)
-- MAGIC spark.udf.register("generate_random_datetime", generate_random_datetime)

-- COMMAND ----------

-- select 
-- "Bronze" as layer,
-- (select count(*) from kpn_bronze.network.cablemodem) as cablemodem,
-- (select count(*) from kpn_bronze.customerservice.alarm) as alarm,
-- (select count(*) from kpn_bronze.customerservice.breakdown) as breakdown,
-- (select count(*) from kpn_bronze.network.inhome) as inhome,
-- (select count(*) from kpn_bronze.network.topology) as topology
-- union
-- select 
-- "Silver" as layer,
-- (select count(*) from kpn_silver.network.cablemodem) as cablemodem,
-- (select count(*) from kpn_silver.customerservice.alarm) as alarm,
-- (select count(*) from kpn_silver.customerservice.breakdown) as breakdown,
-- (select count(*) from kpn_silver.network.inhome) as inhome,
-- (select count(*) from kpn_silver.network.topology) as topology

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df=spark.sql("""select 
-- MAGIC 'Bronze' as layer,
-- MAGIC (select count(*) from kpn_bronze.network.cablemodem) as cablemodem,
-- MAGIC (select count(*) from kpn_bronze.customerservice.alarm) as alarm,
-- MAGIC (select count(*) from kpn_bronze.customerservice.breakdown) as breakdown,
-- MAGIC (select count(*) from kpn_bronze.network.inhome) as inhome,
-- MAGIC (select count(*) from kpn_bronze.network.topology) as topology
-- MAGIC union
-- MAGIC select 
-- MAGIC 'Silver' as layer,
-- MAGIC (select count(*) from kpn_silver.network.cablemodem) as cablemodem,
-- MAGIC (select count(*) from kpn_silver.customerservice.alarm) as alarm,
-- MAGIC (select count(*) from kpn_silver.customerservice.breakdown) as breakdown,
-- MAGIC (select count(*) from kpn_silver.network.inhome) as inhome,
-- MAGIC (select count(*) from kpn_silver.network.topology) as topology""")
-- MAGIC # df=df.write.mode("append").saveAsTable("kpn_test.default.results")
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # %sql
-- MAGIC # ALTER TABLE kpn_test.default.results as
-- MAGIC # ADD column CRTD_DTTM timestamp;

-- COMMAND ----------

select * from kpn_test.default.results order by crtd_dttm desc

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS kpn_silver.customerservice.breakdown_dt(
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
);

CREATE TABLE IF NOT EXISTS kpn_silver.customerservice.rejection_breakdown_dt(
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
);


-- COMMAND ----------

select * from kpn_bronze.network.cablemodem_dt

-- COMMAND ----------

select count(*) from kpn_silver.network.cablemodem_dt

-- COMMAND ----------

Select a.*, a.good + a.bad as tot, round(a.bad/(a.bad+a.good)*100,2) as prcnt_bad from
(
select "cablemodem" as tablename,(select count(*) from kpn_bronze.network.cablemodem_dt where WM_FLAG="Y") as raw,
(select count(*) from kpn_silver.network.rejection_cablemodem_dt where DQ_STATUS = 'O') as bad,
(select count(*) from kpn_silver.network.cablemodem_dt) as good
union all
select "inhome" as tablename,(select count(2) from kpn_bronze.network.inhome_dt where WM_FLAG="Y") as raw,
(select count(*) from kpn_silver.network.rejection_inhome_dt  where DQ_STATUS = 'O') as bad,
(select count(*) from kpn_silver.network.inhome_dt) as good
union all
select "alarm" as tablename,(select count(*) from kpn_bronze.customerservice.alarm_dt where WM_FLAG="Y") as raw,
(select count(*) from kpn_silver.customerservice.rejection_alarm_dt  where DQ_STATUS = 'O') as bad,
(select count(*) from kpn_silver.customerservice.alarm_dt) as good
union all
select "breakdown" as tablename,(select count(*) from kpn_bronze.customerservice.breakdown_dt where WM_FLAG="Y") as raw,
(select count(*) from kpn_silver.customerservice.rejection_breakdown_dt  where DQ_STATUS = 'O') as bad,
(select count(*) from kpn_silver.customerservice.breakdown_dt) as good
) a

-- COMMAND ----------



-- COMMAND ----------

-- truncate table kpn_bronze.network.cablemodem_dt;
-- truncate table kpn_bronze.customerservice.breakdown_dt;
-- truncate table kpn_bronze.customerservice.alarm_dt;
-- truncate table kpn_bronze.network.inhome_dt;

-- truncate table kpn_silver.network.cablemodem_dt;
truncate table kpn_silver.customerservice.breakdown_dt;
-- truncate table kpn_silver.customerservice.alarm_dt;
-- truncate table kpn_silver.network.inhome_dt;

-- truncate table kpn_silver.network.rejection_cablemodem_dt;
truncate table kpn_silver.customerservice.rejection_breakdown_dt;
-- truncate table kpn_silver.customerservice.rejection_alarm_dt;
-- truncate table kpn_silver.network.rejection_inhome_dt;

-- COMMAND ----------

-- UPDATE kpn_bronze.network.cablemodem_dt
-- SET wm_flag = "N";
UPDATE kpn_bronze.customerservice.breakdown_dt
SET wm_flag = "N";
-- UPDATE kpn_bronze.customerservice.alarm_dt
-- SET wm_flag = "N";
-- UPDATE kpn_bronze.network.inhome_dt
-- SET wm_flag = "N";

-- COMMAND ----------

-- Cleaning and Organising all Files
-- vacuum  kpn_silver.network.cablemodem_dt;
-- optimize kpn_silver.network.cablemodem_dt;
-- vacuum kpn_silver.customerservice.breakdown_dt;
-- optimize kpn_silver.customerservice.breakdown_dt;
-- vacuum kpn_silver.customerservice.alarm_dt;
-- optimize kpn_silver.customerservice.alarm_dt;
-- vacuum kpn_silver.network.inhome_dt;
-- optimize kpn_silver.network.inhome_dt;

-- vacuum kpn_silver.network.rejection_cablemodem_dt;
-- optimize kpn_silver.network.rejection_cablemodem_dt;
-- vacuum kpn_silver.customerservice.rejection_breakdown_dt;
-- optimize kpn_silver.customerservice.rejection_breakdown_dt;
-- vacuum kpn_silver.customerservice.rejection_alarm_dt;
-- optimize kpn_silver.customerservice.rejection_alarm_dt;
-- vacuum kpn_silver.network.rejection_inhome_dt;
-- optimize kpn_silver.network.rejection_inhome_dt;

-- COMMAND ----------

-- -- Manual Data Insert
-- INSERT INTO kpn_silver.customerservice.rejection_alarm_dt (
--   ID_ALARM, 
--   DATE_TIME_NOTIF,
--   DATE_TIME,
--   NAME_SUB_SUPPLIER ,
--   GRAVITY,
--   TIPO_NOTIF,
--   COD_LOCAL,
--   TECHNOLOGY_NAME,
--   INS_SUB_SUP,
--   OBJECT,
--   DESC_ALM,
--   ID_NOTIF,
--   MAC_ID,
--   DQ_STATUS,
--   CRTD_DTTM, 
--   CRTD_BY
-- )
-- SELECT 
--   ID_ALARM, 
--   DATE_TIME_NOTIF,
--   DATE_TIME,
--   NAME_SUB_SUPPLIER ,
--   GRAVITY,
--   TIPO_NOTIF,
--   COD_LOCAL,
--   TECHNOLOGY_NAME,
--   INS_SUB_SUP,
--   OBJECT,
--   DESC_ALM,
--   ID_NOTIF,
--   MAC_ID,
--   "O" as DQ_STATUS,
--   CRTD_DTTM, 
--   CRTD_BY
-- FROM kpn_silver.customerservice.rejection_alarm_dt
-- WHERE DQ_STATUS="R";

-- UPDATE kpn_silver.customerservice.rejection_alarm_dt
-- SET DQ_STATUS = 'O'
-- WHERE DQ_STATUS='R';

-- COMMAND ----------

-- select latencyaverage, mac, stationtype  from kpn_silver.network.rejection_inhome_dt
-- select throughputactiveaverage from kpn_silver.network.rejection_inhome_dt

-- update kpn_bronze.network.cablemodem_dt set wm_flag='N';
-- update kpn_bronze.customerservice.breakdown_dt set wm_flag='N';
-- update kpn_bronze.customerservice.alarm_dt set wm_flag='N';
-- update kpn_bronze.network.inhome_dt set wm_flag='N';

-- COMMAND ----------

-- drop table kpn_silver.customerservice.rejection_breakdown_dt;
-- drop table kpn_silver.network.rejection_inhome_dt;
-- drop table kpn_silver.network.rejection_cablemodem_dt ;
-- drop table kpn_silver.customerservice.rejection_alarm_dt; 

-- COMMAND ----------

-- DESCRIBE HISTORY kpn_bronze.network.inhome_dt;

-- COMMAND ----------


-- select 
-- DATE_TIME_NOTIF, CAST(DATE_TIME_NOTIF AS TIMESTAMP) as aa 
-- from kpn_silver.customerservice.rejection_alarm_dt
-- group by DATE_TIME_NOTIF, CAST(DATE_TIME_NOTIF AS TIMESTAMP)


-- select DATE_TIME_NOTIF, count(1)
-- from kpn_bronze.customerservice.alarm_dt
-- group by DATE_TIME_NOTIF

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("s3://uc-kpn-bronze-bucket/network/"))

-- COMMAND ----------

UPDATE kpn_silver.customerservice.rejection_breakdown_dt
SET DQ_STATUS = "O"
WHERE DQ_STATUS="R";

-- COMMAND ----------


