from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'iceberg_job',
    default_args={
        'owner': 'главный',
        'start_date': datetime(2023, 1, 1),
        'retries': 3,
        'retry_delay': None
    },
    schedule_interval=None,
    catchup=False,
) as dag:

    iceberg_task = BashOperator(
        task_id='iceberg_task',
        bash_command=(
            'docker exec '
            '-e AWS_ACCESS_KEY_ID="$MINIO_USER" '
            '-e AWS_SECRET_ACCESS_KEY="$MINIO_PASSWORD" '
            'spark_single bash -c "cd /home/jovyan/work/database && python iceberg.py"'
        ),
)