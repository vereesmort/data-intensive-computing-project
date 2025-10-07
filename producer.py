import json
import time
from kafka import KafkaProducer
import random
from datetime import datetime
import uuid

def generate_warehouse_event():
    event_types = ['order_created', 'inventory_updated', 'shipment_dispatched']
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(event_types),
        "product_id": f"product_{random.randint(1, 100)}",
        "quantity": random.randint(1, 50),
        "timestamp": datetime.utcnow().isoformat(),
        "warehouse_id": f"warehouse_{random.randint(1, 5)}",
        "status": random.choice(['processed', 'pending', 'shipped'])
    }

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    event = generate_warehouse_event()
    print(f"Sending event {event}")
    producer.send('warehouse_events', event)
    time.sleep(1)  # Send event every second