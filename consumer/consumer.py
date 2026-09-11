import json
import struct
import random
import requests
import fastavro
import io
from confluent_kafka import Consumer, Producer, KafkaError

# ── Config ───────────────────────────────────────────────────────
SCHEMA_REGISTRY_URL = "http://localhost:8081"
KAFKA_BROKER          = "localhost:9092"
TOPIC                 = "orders"
DLQ_TOPIC             = "orders-dlq"
MAX_RETRIES           = 3

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

# ── Schema Registry Helper ────────────────────────────────────────
schema_cache = {}

def get_schema(schema_id):
    if schema_id in schema_cache:
        return schema_cache[schema_id]
    url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
    resp = requests.get(url)
    schema_str = resp.json()["schema"]
    parsed = fastavro.parse_schema(json.loads(schema_str))
    schema_cache[schema_id] = parsed
    return parsed

def deserialize_avro(raw_bytes):
    """Decode Confluent wire format: magic byte + 4-byte schema_id + avro bytes"""
    buf = io.BytesIO(raw_bytes)
    magic = buf.read(1)
    if magic != b'\x00':
        raise ValueError("Not a valid Confluent Avro message")
    schema_id = struct.unpack('>I', buf.read(4))[0]
    schema = get_schema(schema_id)
    return fastavro.schemaless_reader(buf, schema)

# ── DLQ Producer ─────────────────────────────────────────────────
dlq_producer = Producer({'bootstrap.servers': KAFKA_BROKER})

def send_to_dlq(order, reason):
    payload = json.dumps({'original_message': order, 'failure_reason': reason})
    dlq_producer.produce(DLQ_TOPIC, value=payload.encode('utf-8'))
    dlq_producer.flush()
    print(f"  💀 Sent to DLQ | reason: {reason}")

# ── Simulated Processing (40% random failure for demo) ────────────
def process_order(order):
    if random.random() < 0.4:
        raise ValueError("Transient processing error (simulated)")
    # Real processing logic would go here
    return True


# ── Consumer Setup ────────────────────────────────────────────────
consumer = Consumer({
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'order-consumer-group',
    'auto.offset.reset': 'earliest'
})
consumer.subscribe([TOPIC])

# ── Running Average State ─────────────────────────────────────────
total_price = 0.0
count = 0

print("🔄 Consumer started — waiting for messages. Press Ctrl+C to stop.\n")

try:
    while True:
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            print(f"❌ Kafka error: {msg.error()}")
            break

        # ── Deserialize ──────────────────────────────────────────
        try:
            order = deserialize_avro(msg.value())
        except Exception as e:
            print(f"❌ Deserialization failed: {e}")
            send_to_dlq(str(msg.value()), f"Deserialization error: {e}")
            continue

        print(f"📨 Received: orderId={order['orderId']}, product={order['product']}, price={order['price']}")

        # ── Retry Logic ──────────────────────────────────────────
        success = False
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                process_order(order)
                success = True
                break
            except ValueError as e:
                print(f"  ⚠️  Attempt {attempt}/{MAX_RETRIES} failed: {e}")

        # ── Result ───────────────────────────────────────────────
        if success:
            total_price += order['price']
            count += 1
            avg = total_price / count
            print(f"  📊 Running Average Price: {avg:.2f}  (over {count} orders processed)")
        else:
            send_to_dlq(order, "Max retries exceeded")

except KeyboardInterrupt:
    print("\n🛑 Consumer stopped.")
finally:
    consumer.close()
