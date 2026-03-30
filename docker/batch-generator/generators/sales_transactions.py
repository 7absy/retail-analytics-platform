import pandas as pd
import random
from faker import Faker
import os
from datetime import timezone

fake = Faker()

CUSTOMER_POOL = [f"CUST_{str(i).zfill(5)}" for i in range(1, 1001)]
PRODUCTS = [f"PROD_{str(i).zfill(4)}" for i in range(1, 501)]
STORES = [f"STORE_{i}" for i in range(1, 51)]

CHANNELS = ["web", "mobile_app", "in_store", "marketplace"]


def generate(output_base: str, date_str: str):

    data = []

    for i in range(1, 5001):

        customer_id = (
            random.choice(CUSTOMER_POOL)
            if random.random() > 0.15
            else None
        )

        store_id = (
            random.choice(STORES)
            if random.random() > 0.30
            else None
        )

        quantity = random.randint(1, 10)
        unit_price = round(random.uniform(5, 1000), 2)
        total_price = round(quantity * unit_price, 2)

        data.append({
            "transaction_id": f"TXN_{str(i).zfill(6)}",
            "order_id": f"ORD_{random.randint(100000, 999999)}",
            "customer_id": customer_id,
            "product_id": random.choice(PRODUCTS),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total_price,
            "transaction_date": fake.date_time_this_year(tzinfo=timezone.utc).isoformat(),
            "store_id": store_id,
            "channel": random.choice(CHANNELS)
        })

    output_path = os.path.join(
        output_base,
        "sales_transactions",
        date_str
    )

    os.makedirs(output_path, exist_ok=True)

    pd.DataFrame(data).to_csv(
        os.path.join(output_path, "sales_transactions.csv"),
        index=False
    )

    print(f"[SUCCESS] sales_transactions written to {output_path}")