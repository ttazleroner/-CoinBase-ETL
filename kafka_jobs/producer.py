import json
import websocket
import boto3
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092', 
value_serializer=lambda 
v: json.dumps(v).encode('utf-8'))

def s3_client(ws):
    sub_topic = {
        "type": "subscribe",
        'products': ['BTC-USD', 'ETH-USD', 'LTC-USD', 'BCH-USD', 'XRP-USD'],
        "channels": ['mathes']
    }
    ws.send(json.dumps(sub_topic))

def on_message(ws, message):
    data = json.loads(message)
    if 'type' in data and data['type'] == 'match':
        producer.send('crypto_topic', value=data)
        print(f"Sent to Kafka: {data}")

ws = websocket.WebSocketApp("wss://ws-feed.exchange.coinbase.com", 
    on_message=on_message,
    on_open=s3_client
)

ws.run_forever()