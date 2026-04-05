from fastapi import APIRouter, Query
from faker import Faker
import random
import json
import os
from datetime import timezone
from azure.eventhub import EventHubProducerClient, EventData

router = APIRouter()
fake = Faker()


def push_to_eventstream(events: list):
    connection_str = os.getenv("EVENTSTREAM_CLICKSTREAM_CONN")
    if not connection_str:
        return
    
    producer = EventHubProducerClient.from_connection_string(connection_str)
    with producer:
        batch = producer.create_batch()
        for event in events:
            batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)


def generate_clickstream_event():
    return {
        "event_id": fake.uuid4(),
        "event_type": random.choice(["PAGE_VIEW", "PRODUCT_CLICK", "ADD_TO_CART", "SEARCH"]),
        "occurred_at": fake.date_time_this_year(tzinfo=timezone.utc).isoformat(),
        "user_id": f"USER_{random.randint(1000,9999)}" if random.random() > 0.3 else None,
        "session_id": f"SES_{fake.uuid4()[:8]}",
        "page_url": f"/product/{random.randint(1000,9999)}",
        "product_id": f"PROD_{random.randint(1000,9999)}" if random.random() > 0.4 else None,
        "device_type": random.choice(["mobile", "desktop", "tablet"]) if random.random() > 0.3 else None,
        "geo_location": fake.country() if random.random() > 0.3 else None
    }


@router.get("/")
def get_clickstream(batch_size: int = Query(50, ge=1, le=500)):
    events = [generate_clickstream_event() for _ in range(batch_size)]

    try:
        push_to_eventstream(events)
    except Exception as e:
        print(f"[WARNING] Eventstream push failed: {e}")

    return {
        "batch_size": batch_size,
        "data": events
    }