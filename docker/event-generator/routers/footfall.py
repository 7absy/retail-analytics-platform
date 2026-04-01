from fastapi import APIRouter, Query
import uuid
import random
from datetime import datetime, timezone

router = APIRouter()

STORE_IDS = [f"STORE_{i}" for i in range(1, 51)]
CUSTOMER_POOL = [f"CUST_{str(i).zfill(5)}" for i in range(1, 1001)]


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def generate_footfall_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(["STORE_ENTRY", "STORE_EXIT"]),
        "occurred_at": iso_now(),
        "store_id": random.choice(STORE_IDS),
        "customer_id": random.choice(CUSTOMER_POOL),

        # FIX ADDED FIELDS
        "customer_type": random.choice(["new", "returning"])
        if random.random() > 0.4 else None,

        "sensor_id": f"SENSOR_{random.randint(1,20)}"
        if random.random() > 0.3 else None
    }


@router.get("/")
def get_footfall_events(batch_size: int = Query(50, ge=1, le=500)):
    return {
        "batch_size": batch_size,
        "data": [generate_footfall_event() for _ in range(batch_size)]
    }