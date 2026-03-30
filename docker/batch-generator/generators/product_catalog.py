import pandas as pd
import random
from faker import Faker
import os

fake = Faker()

PRODUCT_SUFFIX = ["Pro", "Max", "Lite", "Plus"]

CATEGORIES = ["Electronics", "Clothing", "Food", "Home", "Sports"]
SUBCATEGORIES = ["Phones", "Laptops", "Shirts", "Shoes", "Snacks"]


def generate(output_base: str, date_str: str):

    data = []

    for i in range(1, 501):

        base_name = fake.company()
        product_name = f"{base_name} {random.choice(PRODUCT_SUFFIX)}"

        subcategory = random.choice(SUBCATEGORIES) if random.random() > 0.3 else None
        brand = fake.company() if random.random() > 0.2 else None

        price = round(random.uniform(10, 2000), 2)

        cost = (
            round(price * random.uniform(0.4, 0.7), 2)
            if random.random() > 0.2
            else None
        )

        launch_date = (
            fake.date_this_decade().isoformat()
            if random.random() > 0.25
            else None
        )

        data.append({
            "product_id": f"PROD_{str(i).zfill(4)}",
            "product_name": product_name,
            "category": random.choice(CATEGORIES),
            "subcategory": subcategory,
            "brand": brand,
            "price": price,
            "cost": cost,
            "launch_date": launch_date,
            "is_active": True if random.random() < 0.8 else False
        })

    output_path = os.path.join(output_base, "product_catalog", date_str)
    os.makedirs(output_path, exist_ok=True)

    pd.DataFrame(data).to_csv(
        os.path.join(output_path, "product_catalog.csv"),
        index=False
    )

    print(f"[SUCCESS] product_catalog written to {output_path}")