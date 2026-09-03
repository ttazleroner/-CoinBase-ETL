import json
import websocket
from kafka import KafkaProducer

kafka_server = 'kafka_broker:29092'
producer = KafkaProducer(bootstrap_servers=kafka_server, 
value_serializer=lambda 
v: json.dumps(v).encode('utf-8'))

def s3_client(ws):
    sub_topic = {
        "type": "subscribe",
        'product_ids': ['BTC-USD', 'ETH-USD', 'LTC-USD', 'BCH-USD', 'XRP-USD'],
        "channels": ['matches']
    }
    ws.send(json.dumps(sub_topic))

def on_open(ws):
    sub_topic = {
        "type": "subscribe",
        "product_ids": ['BTC-USD', 'ETH-USD', 'LTC-USD', 'BCH-USD', 'XRP-USD'],
        "channels": ['matches']
    }
    ws.send(json.dumps(sub_topic))
    print("Подписка на WebSocket Coinbase отправлена!")

def on_message(ws, message):
    data = json.loads(message)
    if data.get('type') == 'match':
        producer.send('crypto_topic', value=data)
        print(f"[KAFKA <- COINBASE] {data.get('product_id')}: {data.get('price')} USD")

def on_error(ws, error):
    print(f"oшибка WebSocket: {error}")

def on_close(ws, close_status_code, close_msg):
    print("cоединение с Coinbase оффнуто")

ws = websocket.WebSocketApp(
    "wss://ws-feed.exchange.coinbase.com", 
    on_open=on_open,
    on_message=on_message,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever()