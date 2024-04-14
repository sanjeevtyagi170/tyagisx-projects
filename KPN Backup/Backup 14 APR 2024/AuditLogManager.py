from urllib.parse import urljoin
import pandas as pd
import requests
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, LongType
import pyspark.sql.functions as F
from databricks.sdk.runtime import *

class AuditLogManager:

    def create_audit_table_row(self, spark,topic, job_id, run_id, rows_affected_good_table, rows_affected_bad_table, user_name, notebook_path):
        schema = StructType([
            StructField('topic', StringType(), True),
            StructField('job_id', StringType(), True),
            StructField('run_id', StringType(), True),
            StructField('rows_affected_good_table', LongType(), True),
            StructField('rows_affected_bad_table', LongType(), True),
            StructField('user_name', StringType(), True),
            StructField('notebook_path', StringType(), True)
        ])
        data = [(topic, job_id, run_id, rows_affected_good_table, rows_affected_bad_table, user_name, notebook_path)]
        return spark.createDataFrame(data, schema=schema)

    def fetch_job_info_api(self, spark,databricks_host, databricks_token,DBR_JOBS_API_ENDPOINT):
        url = urljoin(databricks_host, DBR_JOBS_API_ENDPOINT)
        headers = {"Authorization": f"Bearer {databricks_token}"}
        response = requests.get(url, headers=headers)
        df = pd.json_normalize(response.json()['runs'])
        df_filtered = df.iloc[:,[0, 1, 5, 9, 12, 17,19]]
        df_filtered.columns = ["job_id", "run_id", "job_start_time", "job_end_time", "job_name", "job_status1","job_status2"]
        df_filtered['job_start_time'] = pd.to_datetime(df_filtered['job_start_time'] / 1000, unit='s')
        df_filtered['job_end_time'] = pd.to_datetime(df_filtered['job_end_time'] / 1000, unit='s')
        max_end_time_job_df = df_filtered[df_filtered['job_name'] == 'kpn_uc_alarm_bronze_to_silver_job']
        max_end_time_job_df = max_end_time_job_df[max_end_time_job_df['job_end_time'] == max(max_end_time_job_df['job_end_time'])]
        schema = StructType([
            StructField('job_id', LongType(), True),
            StructField('run_id', LongType(), True),
            StructField('job_start_time', TimestampType(), True),
            StructField('job_end_time', TimestampType(), True),
            StructField('job_name', StringType(), True),
            StructField('job_status1', StringType(), True),
            StructField('job_status2', StringType(), True)
        ])
        return spark.createDataFrame(max_end_time_job_df, schema=schema)
    
    def handle_source_table_empty(self,spark,topic,DATABRICKS_HOST,DATABRICKS_TOKEN,DBR_JOBS_API_ENDPOINT):
        # if there is no new data found in the source data table the exit from notebook
        try:
            # Check if data is present
            df = spark.sql(f"""select * from kpn_bronze.customerservice.{topic}_dt where wm_flag='NT'""")
            if df.count() == 0:
                raise Exception("No data found in the source table")
        except Exception as e:
            # Handle the exception and log it into the audit table
            print("Exception:", e)
            job_info_df = self.fetch_job_info_api(spark,DATABRICKS_HOST, DATABRICKS_TOKEN,DBR_JOBS_API_ENDPOINT)
            job_info_df = job_info_df.withColumn('job_status2', F.lit("No data found in the source table"))
            job_info_df.write.mode("append").saveAsTable("kpn_silver.default.audit_log_jobs")

            # Optionally, you can exit the notebook
            dbutils.notebook.exit("No data found in the source table")



