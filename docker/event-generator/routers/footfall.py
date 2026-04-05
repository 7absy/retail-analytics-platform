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
    connection_str = os.getenv("EVENTSTREAM_FOOTFALL_CONN")
    if not connection_str:
        return
    
    producer = EventHubProducerClient.from_connection_string(connection_str)
    with producer:
        batch = producer.create_batch()
        for event in events:
            batch.add(EventData(json.dumps(event)))
        producer.send_batch(batch)


def generate_footfall_event():
    return {
        "event_id": fake.uuid4(),
        "event_type": random.choice(["STORE_ENTRY", "STORE_EXIT"]),
        "occurred_at": fake.date_time_this_year(tzinfo=timezone.utc).isoformat(),
        "store_id": f"STORE_{random.randint(1,50)}",
        "customer_id": f"CUST_{str(random.randint(1,1000)).zfill(5)}" if random.random() > 0.2 else None,
        "customer_type": random.choice(["new", "returning"]) if random.random() > 0.4 else None,
        "sensor_id": f"SENSOR_{random.randint(1,20)}" if random.random() > 0.3 else None
    }


@router.get("/")
def get_footfall(batch_size: int = Query(50, ge=1, le=500)):
    events = [generate_footfall_event() for _ in range(batch_size)]

    try:
        push_to_eventstream(events)
    except Exception as e:
        print(f"[WARNING] Eventstream push failed: {e}")

    return {
        "batch_size": batch_size,
        "data": events
    }