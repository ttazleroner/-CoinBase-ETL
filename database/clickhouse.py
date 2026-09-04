import clickhouse
import os
client = clickhouse.Client(
    host='clickhouse-server', port=8123, username='CLICKHOUSE_USER',
    password=os.getenv('CLICKHOUSE_PASSWORD'),
    database='default'
)

client.command("""
    CREATE TABLE IF NOT EXISTS coinbase_orders
    (
        trade_id String,
        product_id String,
        price Float64,
        time DateTime,
        size Float64,
        side String
    )
    ENGINE = MergeTree()
    ORDER BY (product_id, time)
""")