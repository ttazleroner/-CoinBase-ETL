import json
from kafka import KafkaConsumer

TOPIC = 'crypto_topic'
GROUP_ID = 'orders_group'
BOOTSTRAP_SERVERS = ['kafka_broker:29092']
ALERT_THRESHOLD_USD = 30000.0


def deserialize_json(data):
    if not data:
        return None
    try:
        return json.loads(data.decode('utf-8'))
    except Exception as e:
        print(f"ошибка парсинга: {e}")
        return None


def process_transaction_alert(data: dict, threshold_usd: float = ALERT_THRESHOLD_USD):
    if not isinstance(data, dict):
        return None
    try:
        trade_id = data.get('trade_id')
        product_id = data.get('product_id', 'N/A')
        price = float(data.get('price', 0))
        size = float(data.get('size', 0))
        side = data.get('side', 'N/A').upper()
        timestamp = data.get('time', 'N/A')
        volume_usd = price * size

        if volume_usd >= threshold_usd:
            return {
                'trade_id': trade_id,
                'product_id': product_id,
                'volume_usd': round(volume_usd, 2),
                'price': price,
                'size': size,
                'side': side,
                'timestamp': timestamp
            }

    except (ValueError, TypeError):
        pass

    return None

def run_consumer():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        api_version=(3, 7, 0),
        value_deserializer=deserialize_json,
        group_id=GROUP_ID,
        enable_auto_commit=True,
        auto_offset_reset='earliest'
    )
    print("консьюмер запущен")

    try:
        for message in consumer:
            data = message.value
            if not data:
                continue
            alert = process_transaction_alert(data)
            if alert:
                print(
                    f"БОЛЬШАЯ СДЕЛКА | ID: {alert['trade_id']} | "
                    f"монета: {alert['product_id']} | объём: ${alert['volume_usd']:,.2f} | "
                    f"тип: {alert['side']} | цена: ${alert['price']} | кол-во: {alert['size']}"
                )
                
    except KeyboardInterrupt:
        print("консьюмер стоп")
    finally:
        consumer.close()
if __name__ == '__main__':
    run_consumer()