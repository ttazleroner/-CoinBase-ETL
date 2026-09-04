import json
from kafka import KafkaConsumer
import pyspark
from kafka_jobs.consumer import deserialize_json
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from config.env import (
    clickhouse_password,
    iceberg_db_password,
    iceberg_warehouse,
    minio_access_key,
    minio_bucket,
    minio_endpoint,
    minio_secret_key,
)

minio_access_key = minio_access_key()
minio_secret_key = minio_secret_key()
db_pass = iceberg_db_password()
minio_bucket = minio_bucket()
warehouse = iceberg_warehouse()
checkpoints = f"s3a://{minio_bucket}/checkpoints/multi_sink_V1"
clickhouse_pass = clickhouse_password()

spark.context.setLogLevel("WARN")
spark.context.setlogLevel("ERROR")

spark = SparkSession.builder \
    .appName('stream_to_iceberg') \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.postgresql:postgresql:42.6.0,"
            "com.clickhouse:clickhouse-jdbc:0.6.5") \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "jdbc") \
    .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://postgres:5432/airflow") \
    .config("spark.sql.catalog.demo.jdbc.user", "airflow") \
    .config("spark.sql.catalog.demo.jdbc.password", db_pass) \
    .config("spark.sql.catalog.demo.warehouse", warehouse) \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint()) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider") \
    .config("spark.sql.catalog.demo.jdbc.schema-version", "V1") \
    .getOrCreate()