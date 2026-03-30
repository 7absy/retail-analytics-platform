from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import requests
import json

# Ensure project modules are accessible
sys.path.insert(0, "/app")

API_BASE_URL = os.getenv("API_BASE_URL", "http://api-simulator:8000")
OUTPUT_BASE = "/app/data/raw"


# -----------------------
# Default Args
# -----------------------
default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}


# -----------------------
# Helpers
# -----------------------
def ensure_path(path: str):
    os.makedirs(path, exist_ok=True)


# -----------------------
# Tasks
# -----------------------
def fetch_orders(**context):
    date_str = context["ds"]

    page = 1
    all_records = []

    while True:
        response = requests.get(
            f"{API_BASE_URL}/orders",
            params={"page": page, "page_size": 100}
        )
        response.raise_for_status()

        data = response.json()["data"]

        if not data:
            break

        all_records.extend(data)
        page += 1

        if page > 10:  # safety limit
            break

    output_path = f"{OUTPUT_BASE}/orders/{date_str}"
    ensure_path(output_path)

    file_path = f"{output_path}/orders.json"

    with open(file_path, "w") as f:
        json.dump(all_records, f)

    print(f"[SUCCESS] Orders written to {file_path}")


def fetch_campaigns(**context):
    date_str = context["ds"]

    page = 1
    all_records = []

    while True:
        response = requests.get(
            f"{API_BASE_URL}/campaigns",
            params={"page": page, "page_size": 100}
        )
        response.raise_for_status()

        data = response.json()["data"]

        if not data:
            break

        all_records.extend(data)
        page += 1

        if page > 10:
            break

    output_path = f"{OUTPUT_BASE}/campaigns/{date_str}"
    ensure_path(output_path)

    file_path = f"{output_path}/campaigns.json"

    with open(file_path, "w") as f:
        json.dump(all_records, f)

    print(f"[SUCCESS] Campaigns written to {file_path}")


def validate_api_outputs(**context):
    date_str = context["ds"]

    required_files = [
        f"{OUTPUT_BASE}/orders/{date_str}/orders.json",
        f"{OUTPUT_BASE}/campaigns/{date_str}/campaigns.json",
    ]

    missing = [f for f in required_files if not os.path.exists(f)]

    if missing:
        raise FileNotFoundError(f"Missing API output files: {missing}")


def notify_bronze(**context):
    date_str = context["ds"]
    print(f"API data ready for Bronze ingestion — {date_str}")


# -----------------------
# DAG Definition
# -----------------------
with DAG(
    dag_id="api_ingestion_dag",
    default_args=default_args,
    schedule_interval="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["api", "ingestion"]
) as dag:

    fetch_orders_task = PythonOperator(
        task_id="fetch_orders",
        python_callable=fetch_orders
    )

    fetch_campaigns_task = PythonOperator(
        task_id="fetch_campaigns",
        python_callable=fetch_campaigns
    )

    validate_task = PythonOperator(
        task_id="validate_api_outputs",
        python_callable=validate_api_outputs
    )

    notify_task = PythonOperator(
        task_id="notify_bronze",
        python_callable=notify_bronze
    )

    # -----------------------
    # Dependencies (Parallel → Validate → Notify)
    # -----------------------
    [fetch_orders_task, fetch_campaigns_task] >> validate_task >> notify_task