import pandas as pd
import random
from faker import Faker
import os

fake = Faker()

CUSTOMER_IDS = [f"CUST_{str(i).zfill(5)}" for i in range(1, 1001)]


def generate(output_base: str, date_str: str):

    data = []

    for customer_id in CUSTOMER_IDS:

        first_name = fake.first_name()
        last_name = fake.last_name()

        data.append({
            "customer_id": customer_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "gender": random.choice(["male", "female", None]),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=70),
            "registration_date": fake.date_between(start_date="-3y", end_date="today"),
            "loyalty_tier": random.choice(["bronze", "silver", "gold", "platinum"]),
            "country": fake.country()
        })

    output_path = os.path.join(
        output_base,
        "customer_master",
        date_str
    )

    os.makedirs(output_path, exist_ok=True)

    file_path = os.path.join(output_path, "customer_master.csv")

    pd.DataFrame(data).to_csv(file_path, index=False)

    print(f"[SUCCESS] customer_master written to {file_path}")