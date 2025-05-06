from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.bash.operators.bash import BashOperator

default_args = {
    'owner': 'Tom',
    'start_date': datetime.today(),
    'email': ['tom@test.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval='@daily'
)

# Task 1: Unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command='tar -xzf /home/project/airflow/dags/tolldata.tgz -C /home/project/airflow/dags',
    dag=dag
)

# Task 2: Extract data from CSV
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command='cut -d"," -f1,2,3,4 /home/project/airflow/dags/vehicle-data.csv > /home/project/airflow/dags/csv_data.csv',
    dag=dag
)

# Task 3: Extract data from TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command='cut -f5,6,7 /home/project/airflow/dags/tollplaza-data.tsv | tr "\\t" "," > /home/project/airflow/dags/tsv_data.csv',
    dag=dag
)

# Task 4: Extract data from fixed width
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command='cut -c59-61,63-67 /home/project/airflow/dags/payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv',
    dag=dag
)

# Task 5: Consolidate data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command='paste -d"," /home/project/airflow/dags/csv_data.csv /home/project/airflow/dags/tsv_data.csv /home/project/airflow/dags/fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv',
    dag=dag
)

# Task 6: Transform vehicle type field to uppercase
transform_data = BashOperator(
    task_id='transform_data',
    bash_command='cut -d"," -f1-3,4 --complement /home/project/airflow/dags/extracted_data.csv > /home/project/airflow/dags/tmp.csv && cut -d"," -f4 /home/project/airflow/dags/extracted_data.csv | tr "[a-z]" "[A-Z]" > /home/project/airflow/dags/upper.csv && paste -d"," /home/project/airflow/dags/tmp.csv /home/project/airflow/dags/upper.csv > /home/project/airflow/dags/staging/transformed_data.csv && rm /home/project/airflow/dags/tmp.csv /home/project/airflow/dags/upper.csv',
    dag=dag
)

# Task dependencies
unzip_data >> extract_data_from_csv >> consolidate_data
unzip_data >> extract_data_from_tsv >> consolidate_data
unzip_data >> extract_data_from_fixed_width >> consolidate_data
consolidate_data >> transform_data
