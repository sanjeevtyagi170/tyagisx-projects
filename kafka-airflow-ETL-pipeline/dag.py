# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator, PythonOperator
from airflow.utils.dates import days_ago
#defining DAG arguments
default_args = {
    'owner': 'Sanjeev Tyagi',
    'start_date': days_ago(0),# start the DAG today
    'email': ['sanjeev.tyagi170@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),# retry after 5 minutes
}


# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(minutes=1),
)
