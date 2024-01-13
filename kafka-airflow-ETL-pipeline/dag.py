# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

#defining DAG arguments
default_args = {
    'owner': 'Sanjeev Tyagi',
    'start_date': days_ago(0),# start the DAG today
    'email': ['sanjeev.tyagi170@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5) # retry after 5 minutes
}


# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily'
    #schedule_interval=timedelta(minutes=1)
)

# task unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzvf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/staging/',
    dag=dag
)

# task extract csv data
extract_data_from_csv= BashOperator(
    task_id='extract_data_from_csv',
    bash_command="cut -d ',' -f1-4 /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv > /home/project/airflow/dags/finalassignment/staging/csv_data.csv",
    dag=dag
    )

# task extract tsv data
extract_data_from_tsv= BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f 5,6,7 /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv > /home/project/airflow/dags/finalassignment/staging/tsv_data.csv',
    dag=dag
)

# task extract text data
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c 58-62,63-71 /home/project/airflow/dags/finalassignment/staging/payment-data.txt > /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv',
    dag=dag
)

# task combine all data into one
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="paste -d ',' /home/project/airflow/dags/finalassignment/staging/csv_data.csv \
    /home/project/airflow/dags/finalassignment/staging/tsv_data.csv \
    /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv \
    > /home/project/airflow/dags/finalassignment/staging/extracted_data.csv",
    dag=dag
)

# task combine all data into one
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="paste <(cut -d ',' -f 1-3 '/home/project/airflow/dags/finalassignment/staging/extracted_data.csv') \
    <(cut -d ',' -f 4 '/home/project/airflow/dags/finalassignment/staging/extracted_data.csv' \
    | tr '[:lower:]' '[:upper:]') <(cut -d ',' -f 5- '/home/project/airflow/dags/finalassignment/staging/extracted_data.csv') \
    > /home/project/airflow/dags/finalassignment/staging/transformed_data.csv",
    dag=dag
)


# task delete unnecessary files
delete_files = BashOperator(
    task_id='delete_files',
    bash_command='rm /home/project/airflow/dags/finalassignment/staging/csv_data.csv \
    /home/project/airflow/dags/finalassignment/staging/tsv_data.csv \
    /home/project/airflow/dags/finalassignment/staging/fixed_width_data.csv \
    /home/project/airflow/dags/finalassignment/staging/vehicle-data.csv \
    /home/project/airflow/dags/finalassignment/staging/tollplaza-data.tsv \
    /home/project/airflow/dags/finalassignment/staging/payment-data.txt \
    /home/project/airflow/dags/finalassignment/staging/extracted_data.csv \
    /home/project/airflow/dags/finalassignment/staging/fileformats.txt',
    dag=dag
)


unzip_data>>extract_data_from_csv>>extract_data_from_tsv>>extract_data_from_fixed_width>>consolidate_data>>transform_data>>delete_files
