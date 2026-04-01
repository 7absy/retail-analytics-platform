from fastapi import APIRouter, Query
import uuid
import random
from datetime import datetime, timezone

router = APIRouter()

CUSTOMER_POOL = [f"CUST_{str(i).zfill(5)}" for i in range(1, 1001)]
WAREHOUSES = [f"WH_{i}" for i in range(1, 11)]
CARRIERS = ["DHL", "FedEx", "UPS", "Aramex"]

EVENT_FLOW = {
    "ORDER_PLACED": "PLACED",
    "ORDER_CONFIRMED": "CONFIRMED",
    "ORDER_SHIPPED": "SHIPPED",
    "ORDER_DELIVERED": "DELIVERED",
    "ORDER_CANCELLED": "CANCELLED"
}


def iso_now():
    return datetime.now(timezone.utc).isoformat()


def generate_order_event() -> dict:
    event_type = random.choice(list(EVENT_FLOW.keys()))

    status = EVENT_FLOW[event_type]

    return {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "occurred_at": iso_now(),
        "order_id": f"ORD_{str(uuid.uuid4())[:8]}",
        "customer_id": random.choice(CUSTOMER_POOL),
        "status": status,
        "warehouse_id": random.choice(WAREHOUSES) if random.random() > 0.3 else None,
        "carrier": random.choice(CARRIERS) if event_type == "ORDER_SHIPPED" else None
    }


@router.get("/")
def get_order_events(batch_size: int = Query(50, ge=1, le=500)):
    return {
        "batch_size": batch_size,
        "data": [generate_order_event() for _ in range(batch_size)]
    }