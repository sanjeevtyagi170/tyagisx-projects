-- Databricks notebook source
CREATE OR REPLACE TABLE kpn_test.default.cable_modem_bronze
(
  hub STRING,
  mac_addr STRING,
  metrics_date STRING,
  downstream_name STRING,
  cer_dn INT,
  ccer_dn INT,
  snr_dn FLOAT,
  rx_power_dn FLOAT
)

----WHERE to_timestamp(metrics_date,'yyyy/MM/dd HH:mm') IS NOT NULL

-- COMMAND ----------

select to_timestamp(metrics_date,'MM/dd/yyyy hh:mm:ss a') AS metrics_date from kpn_test.default.cable_modem_bronze

-- COMMAND ----------

select * from kpn_test.default.cable_modem_bronze
WHERE metrics_date RLIKE ('^[0-9]{2}/[0-9]{2}/[0-9]{4} [0-9]{2}:[0-9]{2}:[0-9]{2} (AM|PM)$')

-- COMMAND ----------

SELECT regexp_extract('05/03/2018 12:00:00 AM', '\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2}:\\d{2} (AM|PM)') AS regexp_result


-- COMMAND ----------

DELETE FROM kpn_test.default.cable_modem_bronze
--WHERE metrics_date = '2/10/2013 12:48:00 PM'

-- COMMAND ----------

INSERT INTO kpn_test.default.cable_modem_bronze
VALUES('2STL-St_Leonards','6HB17S1D5457','02/10/2013 12:48:00 PM','SWCMT0000161/00015C922072/cable-downstream 8/4/3', 1401188196,937410221,0.9298737,0.8371177),
('4RTL-Red_Tower','3AD18C2E6C68','AM','SWCMT0000158/00015C92206F/cable-downstream 5/1/0',1671095145,1511172700,0.3276527,0.4438011)
-- ('2STL-St_Leonards12','6HB17B1D5B84','05/03/2018 12:00:00 PM','SWCMT0000161/00015C922072/cable-downstream 8/4/3', 1401188196,937410221,0.9298737,0.8371177),
-- ('6RTL-Red_Tower43','5E418C2E6C68','PM','SWCMT0000158/00015C92206F/cable-downstream 5/1/0',1671095145,1511172700,0.3276527,0.4438011)

-- COMMAND ----------

CREATE TABLE kpn_test.default.curation_inhome_bronze
(
  lineid STRING,
  deviceid STRING,
  mac STRING,
  stationtype STRING,
  latencyaverage FLOAT,
  throughputactiveaverage INT,
  rssihistogramcount STRING,
  rssihistogramavgtput STRING,
  rssi INT
)  

-- COMMAND ----------

insert into kpn_test.default.curation_inhome_bronze
-- values('101','18:35:D1:44:04:5B','90:17:C8:8B:C3:AA','Huawei_Phone',4.75,36128918,'[4,1,1,2,2,1,4,17,11,5,8,2,1,2,2,2,1,1,1,1,1,1,2]','[25768,21215,24812,38323,36278,39405,30262,35490,39060,39642,38921,37188,41867,33474,45204,41618,41839,45619,32883,35517,30648,32345,30134]',-73),
-- ('102','18:35:D1:44:04:5B','90:17:C8:8B:C3:AA','Huawei_Phone',4.75,361289,'[4,1,1,2,2,1,4,17,11,5,8,2,1,2,2,2,1,1,1,1,1,1,2]','[25768,21215,24812,38323,36278,39405,30262,35490,39060,39642,38921,37188,41867,33474,45204,41618,41839,45619,32883,35517,30648,32345,30134]',-73),
-- ('18:35:D1:44:04:5B','18:35:D1:44:04:5B','90:17:C8:8B:C3:AA','Huawei_Phone',4.75,361289,'[4,1,1,2,2,1,4,17,11,5,8,2,1,2,2,2,1,1,1,1,1,1,2]','[25768,21215,24812,38323,36278,39405,30262,35490,39060,39642,38921,37188,41867,33474,45204,41618,41839,45619,32883,35517,30648,32345,30134]',-73),
-- ('103','18:35:D1:44:04:5B','90:17:C8:8B:C3:AA','Huawei_Phone',12,361289,'[4,1,1,2,2,1,4,17,11,5,8,2,1,2,2,2,1,1,1,1,1,1,2]','[25768,21215,24812,38323,36278,39405,30262,35490,39060,39642,38921,37188,41867,33474,45204,41618,41839,45619,32883,35517,30648,32345,30134]',-73),
values('18:35:D1:44:04','18:35:D1:44:04:5B',NULL,'Huawei_Phone2',4.75,3612,'[4,1,1,2,2,1,4,17,11,5,8,2,1,2,2,2,1,1,1,1,1,1,2]','[25768,21215,24812,38323,36278,39405,30262,35490,39060,39642,38921,37188,41867,33474,45204,41618,41839,45619,32883,35517,30648,32345,30134]',-73)

-- COMMAND ----------

CREATE OR REPLACE TABLE  kpn_test.default.curation_alarm_bronze(
  id_alarme INT,
  date_time_notif STRING,
  date_time STRING,
  name_sub_supplier STRING,
  gravity STRING,
  tipo_notif STRING,
  cod_local STRING,
  technology_name STRING,
  ins_sub_sup INT,
  object STRING,
  desc_alm STRING,
  id_notif INT,
  MAC_ID STRING
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

-- COMMAND ----------

INSERT INTO kpn_test.default.curation_alarm_bronze
VALUES(36913519,'AD2013-02-10 12:45:40.665279','2/10/2013 12:47','HWI','AVI','I','01CA01','ONT',1,'01CA01/54 0/11/2 SN=5054494E2B8A96B0','ONU in Auto-find state alarm',472716876,NULL)