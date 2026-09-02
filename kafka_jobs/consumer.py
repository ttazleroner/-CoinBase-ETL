from kafka import KafkaConsumer

topic='orders'
group_id='orders_group'

def json(data):
    try:
        return json.json(data.decode('utf-8'))
    except Exception as e:
        print(f'не получилось распарсить {e}')
        return None

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=['kafka_broker:29092'],
    api_version=(3, 7, 0),
    value_deserializer=json,
    group_id=group_id,
    enable_auto_commit=True,
    auto_offset_reset='earliest'
)
try:
    for message in consumer:
        if message.value is not None:
            print(f"получено из кафки: {message.value}")
            transfers = ДОПИСАТЬДОПИСАТЬДОПИСАТЬДОПИСАТЬДОПИСАТЬДОПИСАТЬДОПИСАТЬДОПИСАТЬ
ДОПИСАТЬДОПИСАТЬДОПИСАТЬДОПИСАТЬ
ДОПИСАТЬДОПИСАТЬДОПИСАТЬ
ДОПИСАТЬДОПИСАТЬДОПИСАТЬ
ДОПИСАТЬ
ДОПИСАТЬ
ДОПИСАТЬ
ДОПИСАТЬ
ДОПИСАТЬ
ДОПИСАТЬ
ДОПИСАТЬ
ДОПИСАТЬДОПИСАТЬ

ДОПИСАТЬ
