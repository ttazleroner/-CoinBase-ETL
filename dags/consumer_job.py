from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def_args = {
    'owner': 'главный',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

SPARK_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "org.postgresql:postgresql:42.6.0,"
    "com.clickhouse:clickhouse-jdbc:0.5.0"
)

SPARK_COMMAND = f"""
docker exec -i \
  -e ICEBERG_DB_PASS="$ICEBERG_DB_PASS" \
  -e AWS_ACCESS_KEY_ID="$MINIO_USER" \
  -e AWS_SECRET_ACCESS_KEY="$MINIO_PASSWORD" \
  -e CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD" \
  spark_single spark-submit --packages {SPARK_PACKAGES} /home/jovyan/work/kafka_jobs/consumer.py
"""

def end_msg():
    print("консьюмер завершил работу")

with DAG(
    'consumer',
    default_args=def_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    consumer_kafka = BashOperator(
        task_id='consumer_kafka',
        bash_command=(
            'docker exec '
            '-e AWS_ACCESS_KEY_ID="$MINIO_USER" '
            '-e AWS_SECRET_ACCESS_KEY="$MINIO_PASSWORD" '
            'spark_single bash -c "cd /home/jovyan/work/kafka_jobs && python consumer.py"'
        ),
    )

    end_task = PythonOperator(
        task_id='end_message',
        python_callable=end_msg
    )

    consumer_kafka >> end_task