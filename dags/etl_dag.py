import sys
import os

# Agregar la ruta al paquete src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importar funciones desde los módulos en src
from API_connection import get_data_from_api
from API_cleaning import clean_api_data
from extract import extract_data
from data_cleaning import clean_data
from upload_to_postgres import upload_to_postgres
from upload_to_drive import upload_to_drive
from merge_data import merge_data

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
    'accidents_etl_pipeline',
    default_args=default_args,
    description='ETL Pipeline for Accidents Data',
    schedule_interval='@daily',
) as dag:

    # Definir las tareas
    
    get_data_task = PythonOperator(
        task_id='get_data_from_api',
        python_callable=get_data_from_api,
        dag=dag,
    )

    clean_api_data_task = PythonOperator(
        task_id='clean_api_data',
        python_callable=clean_api_data,
        dag=dag,
    )

    extract_csv_task = PythonOperator(
        task_id='extract_csv',
        python_callable=extract_data,
        dag=dag,
    )

    clean_data_task = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
        dag=dag,
    )

    merge_data_task = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data,
        dag=dag,
    )

    upload_to_postgres_task = PythonOperator(
        task_id='upload_to_postgres',
        python_callable=upload_to_postgres,
        dag=dag,
    )

    upload_to_drive_task = PythonOperator(
        task_id='upload_to_drive',
        python_callable=upload_to_drive,
        dag=dag,
    )

    # Definir la secuencia de tareas
    get_data_task >> clean_api_data_task >> merge_data_task
    extract_csv_task >> clean_data_task >> merge_data_task >> upload_to_postgres_task >> upload_to_drive_task
