import pandas as pd
import random
from faker import Faker
import os

fake = Faker()

PRODUCTS = [f"PROD_{str(i).zfill(4)}" for i in range(1, 501)]
STORES = [f"STORE_{i}" for i in range(1, 51)]
WAREHOUSES = [f"WH_{i}" for i in range(1, 11)]


def generate(output_base: str, date_str: str):

    data = []

    for product_id in PRODUCTS:
        for store_id in STORES:

            stock_on_hand = random.randint(0, 500)

            stock_reserved = (
                random.randint(0, stock_on_hand)
                if random.random() > 0.3
                else None
            )

            reorder_level = (
                random.randint(10, 100)
                if random.random() > 0.3
                else None
            )

            warehouse_id = (
                random.choice(WAREHOUSES)
                if random.random() > 0.4
                else None
            )

            data.append({
                "snapshot_date": date_str,
                "product_id": product_id,
                "store_id": store_id,
                "warehouse_id": warehouse_id,
                "stock_on_hand": stock_on_hand,
                "stock_reserved": stock_reserved,
                "reorder_level": reorder_level
            })

    output_path = os.path.join(output_base, "inventory_snapshot", date_str)
    os.makedirs(output_path, exist_ok=True)

    pd.DataFrame(data).to_csv(
        os.path.join(output_path, "inventory_snapshot.csv"),
        index=False
    )

    print(f"[SUCCESS] inventory_snapshot written to {output_path}")