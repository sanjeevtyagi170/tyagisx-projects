# Databricks notebook source
import datetime
import random
 
def generate_random_datetime():
  min_year = 2024
  max_year = datetime.datetime.today().year
  min_month = 1
  max_month = datetime.datetime.today().month
  min_day = 1
  max_day = datetime.datetime.today().day
 
  year = random.randint(min_year, max_year)
  month = random.randint(min_month,max_month)
  day = random.randint(min_day, max_day)
  hour = random.randint(0, 23)
  minute = random.randint(0, 59)
  second = random.randint(0, 59)
  microsecond = random.randint(0, 999999)
  random_datetime = datetime.datetime(year, month, day, hour, minute, second, microsecond)
  formatted_datetime = random_datetime.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
 
  return formatted_datetime
random_datetime_str = generate_random_datetime()
 
print(random_datetime_str)

# COMMAND ----------

spark.udf.register("date_udf", generate_random_datetime)