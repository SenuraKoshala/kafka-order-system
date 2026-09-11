import random
import time
import json
import struct
import requests
import fastavro
import io
from confluent_kafka import Producer

# ── Schema Registry ──────────────────────────────────────────────
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"

AVRO_SCHEMA = {
    "type": "record",
    "name": "Order",
    "namespace": "com.example",
    "fields": [
        {"name": "orderId",  "type": "string"},
        {"name": "product",  "type": "string"},
        {"name": "price",    "type": "float"}
    ]
}

def register_schema():
    url = f"{SCHEMA_REGISTRY_URL}/subjects/{TOPIC}-value/versions"
    payload = {"schema": json.dumps(AVRO_SCHEMA)}
    resp = requests.post(url, json=payload, headers={"Content-Type": "application/vnd.schemaregistry.v1+json"})
    schema_id = resp.json()["id"]
    print(f"✅ Schema registered with ID: {schema_id}")
    return schema_id

def serialize_avro(record, schema_id):
    """Serialize record using Confluent wire format: magic byte + schema_id + avro bytes"""
    schema = fastavro.parse_schema(AVRO_SCHEMA)
    buf = io.BytesIO()
    buf.write(b'\x00')                        # Magic byte
    buf.write(struct.pack('>I', schema_id))   # 4-byte schema ID (big-endian)
    fastavro.schemaless_writer(buf, schema, record)
    return buf.getvalue()

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Sent → topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")

# ── Main ─────────────────────────────────────────────────────────
producer = Producer({'bootstrap.servers': 'localhost:9092'})
schema_id = register_schema()

products = ['Item1', 'Item2', 'Item3', 'Item4', 'Item5']
order_id = 1001

print("\n🚀 Producer started — sending 1 order every 2 seconds. Press Ctrl+C to stop.\n")

try:
    while True:
        order = {
            'orderId': str(order_id),
            'product': random.choice(products),
            'price':   round(random.uniform(10.0, 500.0), 2)
        }
        print(f"📦 Producing: orderId={order['orderId']}, product={order['product']}, price={order['price']}")

        serialized = serialize_avro(order, schema_id)
        producer.produce(TOPIC, value=serialized, callback=delivery_report)
        producer.poll(0)

        order_id += 1
        time.sleep(2)

except KeyboardInterrupt:
    print("\n🛑 Producer stopped.")
    producer.flush()
