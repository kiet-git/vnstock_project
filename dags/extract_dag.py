from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
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
    schedule= "5 13 * * *",
    tags=["extract"],
)

#Uncomment this to trigger this job when the directory sensor dag complete
# wait_directory_sensor = ExternalTaskSensor(
#     task_id='wait_directory_sensor',
#     external_dag_id='directory_sensor_dag',
#     external_task_id='move_files_task',
#     start_date=days_ago(1),
#     execution_delta=timedelta(minutes=5),
#     timeout=3600,
#     mode="reschedule",
#     check_existence=True
# )

extract = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    op_args=['/opt/airflow/dags/modules/tmp/'],
    provide_context=True,
    dag=dag,
    retries=5,
    retry_delay=timedelta(minutes=1),
)

store = BashOperator(
    task_id='store_data_task',
    bash_command='{{ task_instance.xcom_pull(task_ids=\'extract_data_task\', key=\'bash_command\')}}',
    dag=dag,
)

delete_tmp = BashOperator(
    task_id='delete_tmp_task',
    bash_command='rm -rf /opt/airflow/dags/modules/tmp/*',
    dag=dag,
)

#Uncomment this to trigger this job when the directory sensor dag complete
# wait_directory_sensor >> extract >> store >> delete_tmp
extract >> store >> delete_tmp