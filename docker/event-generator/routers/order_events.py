from fastapi import APIRouter, Query
from faker import Faker
import random
import json
import os
from datetime import timezone
from azure.eventhub import EventHubProducerClient, EventData

router = APIRouter()
fake = Faker()

CUSTOMER_POOL = [f"CUST_{str(i).zfill(5)}" for i in range(1, 1001)]

EVENT_FLOW = {
    "ORDER_PLACED": "PENDING",
    "ORDER_CONFIRMED": "CONFIRMED",
    "ORDER_SHIPPED": "SHIPPED",
    "ORDER_DELIVERED": "DELIVERED",
    "ORDER_CANCELLED": "CANCELLED",
}


def push_to_eventstream(events: list):
    connection_str = os.getenv("EVENTSTREAM_ORDERS_CONN")
    if not connection_str:
        return
    
    producer = EventHubProducerClient.from_connection_string(connection_str)
    with producer:
        batch = producer.create_batch()
        for event in events:
            batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)


def generate_order_event():
    event_type = random.choice(list(EVENT_FLOW.keys()))

    return {
        "event_id": fake.uuid4(),
        "event_type": event_type,
        "occurred_at": fake.date_time_this_year(tzinfo=timezone.utc).isoformat(),
        "order_id": f"ORD_{random.randint(100000,999999)}",
        "customer_id": random.choice(CUSTOMER_POOL),
        "status": EVENT_FLOW[event_type],
        "warehouse_id": f"WH_{random.randint(1,10)}" if random.random() > 0.5 else None,
        "carrier": fake.company() if event_type == "ORDER_SHIPPED" else None
    }


@router.get("/")
def get_order_events(batch_size: int = Query(50, ge=1, le=500)):
    events = [generate_order_event() for _ in range(batch_size)]

    try:
        push_to_eventstream(events)
    except Exception as e:
        print(f"[WARNING] Eventstream push failed: {e}")

    return {
        "batch_size": batch_size,
        "data": events
    }