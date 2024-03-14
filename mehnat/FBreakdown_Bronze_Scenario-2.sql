-- Databricks notebook source
CREATE OR REFRESH STREAMING LIVE TABLE breakdown
COMMENT 'Ingesting raw data from s3 bucket to bronze table'
TBLPROPERTIES ("quality" = "bronze")
AS 
SELECT * FROM cloud_files('s3://uc-kpn-landing-bucket/customerservice/breakdown/*/*/*/*/','parquet')