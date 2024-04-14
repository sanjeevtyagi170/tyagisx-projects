SET spark.sql.legacy.timeParserPolicy = LEGACY;

CREATE OR REFRESH STREAMING LIVE TABLE alarm_slt
(
  CONSTRAINT valid_date_time_notif EXPECT(CAST(DATE_TIME_NOTIF AS TIMESTAMP) IS NOT NULL AND LEN(TIPO_NOTIF)=1) ON VIOLATION DROP ROW
  --CONSTRAINT valid_date_time_notif EXPECT(DATE_TIME_NOTIF RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}'))ON VIOLATION DROP ROW
)
AS
SELECT 
  ID_ALARM,
  to_timestamp(DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
  to_timestamp(DATE_TIME,'yyyy-MM-dd HH:mm:SS') AS DATE_TIME,
  NAME_SUB_SUPPLIER,
  GRAVITY,
  TIPO_NOTIF,
  COD_LOCAL,
  TECHNOLOGY_NAME,
  INS_SUB_SUP,
  OBJECT,
  DESC_ALM,
  ID_NOTIF,
  MAC_ID,
  current_timestamp() AS CRTD_DTTM,
  'data_engineer_group' AS CRTD_BY
FROM STREAM(kpn_bronze.customerservice.alarm_dt);

CREATE OR REFRESH STREAMING LIVE TABLE rejection_alarm_slt
(
  CONSTRAINT valid_date_time_notif EXPECT(CAST(DATE_TIME_NOTIF AS TIMESTAMP) IS NULL OR LEN(TIPO_NOTIF)>1) ON VIOLATION DROP ROW
  --CONSTRAINT valid_date_time_notif EXPECT(DATE_TIME_NOTIF NOT RLIKE ('^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}')) ON VIOLATION DROP ROW,
)
AS
SELECT 
  ID_ALARM,
  to_timestamp(DATE_TIME_NOTIF) AS DATE_TIME_NOTIF,
  to_timestamp(DATE_TIME,'MM/dd/yyyy hh:mm a') AS DATE_TIME,
  NAME_SUB_SUPPLIER,
  GRAVITY,
  TIPO_NOTIF,
  COD_LOCAL,
  TECHNOLOGY_NAME,
  INS_SUB_SUP,
  OBJECT,
  DESC_ALM,
  ID_NOTIF,
  MAC_ID,
  current_timestamp() AS Rejection_DTTM,
  'data_engineer_group' AS Rejected_BY
FROM STREAM(kpn_bronze.customerservice.alarm_dt);


CREATE OR REFRESH LIVE table transformation_dt
AS
(SELECT NAME_SUB_SUPPLIER,count(*) AS good_record_count FROM (live.alarm_slt) GROUP BY NAME_SUB_SUPPLIER) a
JOIN 
(SELECT NAME_SUB_SUPPLIER,count(*) AS bad_record_count  FROM (live.rejection_alarm_slt) GROUP BY NAME_SUB_SUPPLIER)b
ON a.NAME_SUB_SUPPLIER = b.NAME_SUB_SUPPLIER