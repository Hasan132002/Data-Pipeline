from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os
from datetime import timedelta

default_args = {
    'owner': 'hasan',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='so_pipeline_mongo_pinecone',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    def run_script(script):
        os.system(f"python /opt/airflow/dags/scripts/{script}")

    ingest = PythonOperator(task_id='ingest', python_callable=lambda: run_script('ingest.py'))
    preprocess = PythonOperator(task_id='preprocess', python_callable=lambda: run_script('preprocess.py'))
    hf_infer = PythonOperator(task_id='hf_inference', python_callable=lambda: run_script('hf_inference.py'))

    ingest >> preprocess >> hf_infer
