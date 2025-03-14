from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from datetime import datetime

# Define the paths for source scripts
SRC_PATH = '/opt/airflow/src/'

# Import functions from the source scripts
from src.data_ingestion import load_data_to_stagging
from src.data_validation import validate_data
from src.data_preparation import prepare_data
from src.data_transformation import transform_data
from src.feature_store import create_feature_store
from src.model_training import build_and_train_model

# Define the DAG
dag = DAG(
    'churn_prediction_pipeline',
    description='A simple customer churn prediction pipeline',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Airflow tasks for each pipeline stage
ingest_data = PythonOperator(
    task_id='download_data',
    python_callable=load_data_to_stagging,
    dag=dag
)

validate_data_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)

prepare_data_task = PythonOperator(
    task_id='prepare_data',
    python_callable=prepare_data,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

feature_store_task = PythonOperator(
    task_id='create_feature_store',
    python_callable=create_feature_store,
    dag=dag
)

model_task = PythonOperator(
    task_id='build_and_train_model',
    python_callable=build_and_train_model,
    dag=dag
)

# Set task dependencies
ingest_data >> validate_data_task >> prepare_data_task >> transform_data_task >> feature_store_task >> model_task
