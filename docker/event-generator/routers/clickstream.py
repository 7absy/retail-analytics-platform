from fastapi import APIRouter, Query
import uuid
import random
from datetime import datetime, timezone

router = APIRouter()

CUSTOMER_POOL = [f"CUST_{str(i).zfill(5)}" for i in range(1, 1001)]

EVENT_TYPES = ["PAGE_VIEW", "PRODUCT_CLICK", "ADD_TO_CART", "SEARCH"]


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def generate_click_event() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": random.choice(EVENT_TYPES),
        "occurred_at": iso_now(),
        "customer_id": random.choice(CUSTOMER_POOL),
        "session_id": f"SES_{str(uuid.uuid4())[:8]}",
        "page_url": f"/product/{random.randint(1000, 9999)}",
        "device_type": random.choice(["mobile", "desktop", "tablet"]),

        # FIX 1: added product_id (nullable)
        "product_id": f"PROD_{random.randint(1000,9999)}"
        if random.random() > 0.4 else None,

        # FIX 2: added geo_location (nullable)
        "geo_location": random.choice(["USA", "UK", "Germany", "UAE", "India"])
        if random.random() > 0.3 else None
    }


@router.get("/")
def get_clickstream_events(batch_size: int = Query(50, ge=1, le=500)):
    return {
        "batch_size": batch_size,
        "data": [generate_click_event() for _ in range(batch_size)]
    }