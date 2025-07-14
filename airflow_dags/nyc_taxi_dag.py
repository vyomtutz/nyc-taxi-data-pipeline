from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG('nyc_taxi_pipeline', start_date=datetime(2023, 1, 1), schedule_interval='@daily', catchup=False) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Start pipeline"'
    )

    batch_processing = BashOperator(
        task_id='run_pyspark_batch',
        bash_command='spark-submit /path/to/pyspark_batch_job.py'
    )

    quality_check = BashOperator(
        task_id='run_quality_check',
        bash_command='python /path/to/custom_quality_checks.py'
    )

    start >> batch_processing >> quality_check
