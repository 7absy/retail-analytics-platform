from fastapi import APIRouter
from faker import Faker
import random
from datetime import datetime, timedelta, timezone

router = APIRouter()
fake = Faker()

CHANNELS = ["email", "sms", "social", "paid_ads"]

# FIX 3: match contract exactly
STATUSES = ["ACTIVE", "PAUSED", "COMPLETED", "DRAFT"]

TARGET_AUDIENCES = [
    "young_adults",
    "professionals",
    "parents",
    "students",
    "high_income_users"
]


def generate_single_campaign(i: int, page: int, page_size: int) -> dict:
    # FIX 1: UTC timestamps
    start_date = fake.date_time_this_year(tzinfo=timezone.utc)

    end_date = (
        start_date + timedelta(days=random.randint(10, 90))
        if random.random() > 0.3
        else None
    )

    # FIX 2: global unique ID across pages
    global_index = (page - 1) * page_size + i

    return {
        "campaign_id": f"CMP_{str(global_index).zfill(4)}",
        "campaign_name": fake.catch_phrase(),
        "channel": random.choice(CHANNELS),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat() if end_date else None,
        "budget": round(random.uniform(5000, 100000), 2) if random.random() > 0.2 else None,
        "target_audience": random.choice(TARGET_AUDIENCES),
        "status": random.choice(STATUSES)
    }


@router.get("/")
def get_campaigns(page: int = 1, page_size: int = 100):
    campaigns = [
        generate_single_campaign(i, page, page_size)
        for i in range(page_size)
    ]

    return {
        "page": page,
        "page_size": page_size,
        "data": campaigns
    }