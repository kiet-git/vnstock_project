import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from modules.extract import extract_data

date_string = datetime.now().strftime("%d-%m-%Y")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 10,
    'retry_delay': timedelta(hours=1),
}

dag = DAG(
    'extract_dag',
    default_args=default_args,
    description='DAG to extract data',
    schedule_interval= "0 16 * * *"
)

wait_directory_sensor = ExternalTaskSensor(
    task_id='wait_directory_sensor',
    external_dag_id='directory_sensor_dag',
    external_task_id='move_files_task',
    start_date=days_ago(1),
    execution_delta=timedelta(hours=3),
    timeout=3600,
    mode="reschedule",
    check_existence=True
)

bash_command = extract_data()
extract = BashOperator(
    task_id='extract_task',
    bash_command=bash_command,
    dag=dag,
)

delete_tmp = BashOperator(
    task_id='delete_tmp_task',
    bash_command='rm -f /opt/airflow/dags/modules/tmp/*',
    dag=dag,
)

wait_directory_sensor >> extract >> delete_tmp