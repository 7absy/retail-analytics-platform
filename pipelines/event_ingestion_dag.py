from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import requests
import json

# Ensure project modules are accessible
sys.path.insert(0, "/app")

EVENT_BASE_URL = os.getenv("EVENT_BASE_URL", "http://event-generator:9000")
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
def fetch_order_events(**context):
    date_str = context["ds"]

    response = requests.get(
        f"{EVENT_BASE_URL}/events/orders",
        params={"batch_size": 100}
    )
    response.raise_for_status()

    data = response.json()["data"]

    output_path = f"{OUTPUT_BASE}/events/order_events/{date_str}"
    ensure_path(output_path)

    file_path = f"{output_path}/order_events.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    print(f"[SUCCESS] Order events written to {file_path}")


def fetch_clickstream(**context):
    date_str = context["ds"]

    response = requests.get(
        f"{EVENT_BASE_URL}/events/clickstream",
        params={"batch_size": 100}
    )
    response.raise_for_status()

    data = response.json()["data"]

    output_path = f"{OUTPUT_BASE}/events/clickstream/{date_str}"
    ensure_path(output_path)

    file_path = f"{output_path}/clickstream.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    print(f"[SUCCESS] Clickstream events written to {file_path}")


def fetch_footfall(**context):
    date_str = context["ds"]

    response = requests.get(
        f"{EVENT_BASE_URL}/events/footfall",
        params={"batch_size": 100}
    )
    response.raise_for_status()

    data = response.json()["data"]

    output_path = f"{OUTPUT_BASE}/events/footfall/{date_str}"
    ensure_path(output_path)

    file_path = f"{output_path}/footfall.json"

    with open(file_path, "w") as f:
        json.dump(data, f)

    print(f"[SUCCESS] Footfall events written to {file_path}")


def validate_event_outputs(**context):
    date_str = context["ds"]

    required_files = [
        f"{OUTPUT_BASE}/events/order_events/{date_str}/order_events.json",
        f"{OUTPUT_BASE}/events/clickstream/{date_str}/clickstream.json",
        f"{OUTPUT_BASE}/events/footfall/{date_str}/footfall.json",
    ]

    missing = [f for f in required_files if not os.path.exists(f)]

    if missing:
        raise FileNotFoundError(f"Missing event files: {missing}")


def notify_bronze(**context):
    date_str = context["ds"]
    print(f"Event data ready for Bronze ingestion — {date_str}")


# -----------------------
# DAG Definition
# -----------------------
with DAG(
    dag_id="event_ingestion_dag",
    default_args=default_args,
    schedule_interval="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["events", "ingestion"]
) as dag:

    fetch_order_events_task = PythonOperator(
        task_id="fetch_order_events",
        python_callable=fetch_order_events
    )

    fetch_clickstream_task = PythonOperator(
        task_id="fetch_clickstream",
        python_callable=fetch_clickstream
    )

    fetch_footfall_task = PythonOperator(
        task_id="fetch_footfall",
        python_callable=fetch_footfall
    )

    validate_task = PythonOperator(
        task_id="validate_event_outputs",
        python_callable=validate_event_outputs
    )

    notify_task = PythonOperator(
        task_id="notify_bronze",
        python_callable=notify_bronze
    )

    # -----------------------
    # Dependencies (Parallel → Validate → Notify)
    # -----------------------
    [
        fetch_order_events_task,
        fetch_clickstream_task,
        fetch_footfall_task
    ] >> validate_task >> notify_task