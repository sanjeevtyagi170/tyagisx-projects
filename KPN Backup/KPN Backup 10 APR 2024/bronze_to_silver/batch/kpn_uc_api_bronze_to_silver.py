# Databricks notebook source
import requests
import json
from aws_requests_auth.aws_auth import AWSRequestsAuth

# This Sample API only gets new data which will be saved to Bronze Delta Table
API_URL = "https://4azatvdrb5.execute-api.eu-central-1.amazonaws.com/Datagen_app"
HOST = "4azatvdrb5.execute-api.eu-central-1.amazonaws.com"
KEY = dbutils.secrets.get(scope="kpn_scope", key="AWS_KEY")
SECRET = dbutils.secrets.get(scope="kpn_scope", key="AWS_SECRET")

table_name = "kpn_bronze.customerservice.breakdown_api"

auth = AWSRequestsAuth(aws_access_key=KEY,
                       aws_secret_access_key=SECRET,
                       aws_host=HOST,
                       aws_region='eu-central-1',
                       aws_service='execute-api')

res = requests.get(API_URL, auth=auth)

df = spark.createDataFrame(json.loads(res.content))

df.write.mode("append").saveAsTable(table_name)