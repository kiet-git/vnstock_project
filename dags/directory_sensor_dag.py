from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 3),
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(hours=1),
}

dag = DAG(
    'directory_sensor_dag',
    default_args=default_args,
    description='DAG to trigger when a directory has a new file',
    schedule_interval='@daily',
)

directory_sensor_task = FileSensor(
    task_id='sense_directory',
    fs_conn_id='airflow_db',
    filepath='outputs/',
    dag=dag,
)

bash_command = 'docker exec -it namenode bash -c "hdfs dfs -put /opt/airflow/data/outputs/* /user/data/outputs/"'

move_files_task = BashOperator(
    task_id='move_files_task',
    bash_command=bash_command,
    dag=dag,
)

directory_sensor_task >> move_files_task