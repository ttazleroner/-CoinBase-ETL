import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config.config import (
    iceberg_db_password,
    iceberg_warehouse,
    minio_access_key,
    minio_endpoint,
    minio_secret_key,
)
minio_access_key = minio_access_key()
minio_secret_key = minio_secret_key()
DB_PASS = iceberg_db_password()

ICEBERG_PACKAGES = [
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "org.postgresql:postgresql:42.6.0"
]

spark = SparkSession.builder \
    .appName("IcebergDatabase") \
    .config("spark.sql.catalog.demo.jdbc.password", DB_PASS) \
    .config("spark.sql.catalog.demo.jdbc.schema-version", "V1") \
    \
    .config("spark.sql.catalog.demo", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.demo.type", "jdbc") \
    .config("spark.sql.catalog.demo.uri", "jdbc:postgresql://postgres:5432/airflow") \
    .config("spark.sql.catalog.demo.jdbc.user", "coinbase") \
    .config("spark.sql.catalog.demo.warehouse", iceberg_warehouse()) \
    .config("spark.sql.catalog.demo.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO") \
    \
    .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint()) \
    .config("spark.hadoop.fs.s3a.access.key", minio_access_key()) \
    .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key()) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    \
    .getOrCreate()

spark.sql("""
    CREATE TABLE IF NOT EXISTS demo.coinbase_orders (
    trade_id STRING,
    product_id STRING,
    price DOUBLE,
    time Timestamp,
    size DOUBLE,
    side STRING
    )
    USING iceberg
    PARTITIONED BY (days(time))
""")


spark.sql("""
    ALTER TABLE demo.coinbase_orders SET TBLPROPERTIES (
        'write.target-file-size-bytes' = '134217728',
        'write.distribution-mode' = 'hash',
        'write.spark.fanout.enabled' = 'true'
    )
""")

spark.sql("""
    SELECT * FROM demo.coinbase_orders
    LIMIT 20
""").show(truncate=False)