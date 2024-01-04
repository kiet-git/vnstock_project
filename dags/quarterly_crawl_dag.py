from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="quarterly_crawl_dag",
    schedule='0 18 1 */3 *',
    start_date=datetime(2023, 1, 3),
    default_args=default_args,
    tags=["crawl"],
    description='DAG to craw data quaterly'
) as dag:
    from modules.data_crawling import capture_all_data

    single_task = PythonOperator(
        task_id='single_task',
        python_callable=capture_all_data,
        op_args=[datetime.now().date(), 10, 1],
        dag=dag,
    )
