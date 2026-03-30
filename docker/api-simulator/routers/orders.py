from fastapi import APIRouter
from faker import Faker
import random
from datetime import timezone

router = APIRouter()
fake = Faker()

CUSTOMER_POOL = [f"CUST_{str(i).zfill(5)}" for i in range(1, 1001)]

ORDER_STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"]
CHANNELS = ["web", "mobile_app", "in_store", "marketplace"]
PAYMENT_METHODS = ["credit_card", "debit_card", "wallet", "cash_on_delivery"]

def generate_single_order() -> dict:
    return {
        "order_id": fake.uuid4(),
        "customer_id": random.choice(CUSTOMER_POOL),
        "order_status": random.choice(ORDER_STATUSES),
        "order_date": fake.date_time_this_year(tzinfo=timezone.utc).isoformat(),
        "channel": random.choice(CHANNELS),
        "total_amount": round(random.uniform(10, 500), 2),
        "currency": "USD",
        "discount_amount": round(random.uniform(0, 50), 2) if random.random() > 0.5 else None,
        "shipping_cost": round(random.uniform(5, 20), 2),
        "payment_method": random.choice(PAYMENT_METHODS),
        "shipping_address": fake.address() if random.random() > 0.2 else None
    }
    
@router.get("/")
def get_orders(page: int = 1, page_size: int = 100):
    orders = [generate_single_order() for _ in range(page_size)]
    return {
        "page": page,
        "page_size": page_size,
        "data": orders
    }