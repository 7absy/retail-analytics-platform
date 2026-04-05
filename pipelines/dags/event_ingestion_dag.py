from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import requests
import json

# Ensure project modules are accessible
sys.path.insert(0, "/opt/airflow/pipelines")

EVENT_BASE_URL = os.getenv("EVENT_BASE_URL", "http://event-generator:9000")
TEMP_BASE = "/tmp"


# -----------------------
# ADLS Upload Helper
# -----------------------
from azure.storage.blob import BlobServiceClient

def upload_to_adls(local_path: str, blob_path: str):
    account_name = os.getenv("ADLS_ACCOUNT_NAME")
    account_key  = os.getenv("ADLS_ACCOUNT_KEY")
    
    client = BlobServiceClient(
        account_url=f"https://{account_name}.blob.core.windows.net",
        credential=account_key
    )
    
    blob_client = client.get_blob_client(
        container="raw",
        blob=blob_path
    )
    
    with open(local_path, "rb") as f:
        blob_client.upload_blob(f, overwrite=True)
    
    print(f"[UPLOADED] {blob_path}")


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

    local_dir = f"{TEMP_BASE}/events/order_events/{date_str}"
    ensure_path(local_dir)

    local_file = f"{local_dir}/order_events.json"

    # Save locally
    with open(local_file, "w") as f:
        json.dump(data, f)

    # Upload to ADLS
    blob_path = f"events/order_events/{date_str}/order_events.json"
    upload_to_adls(local_file, blob_path)

    # Cleanup
    os.remove(local_file)

    print(f"[SUCCESS] Order events processed")


def fetch_clickstream(**context):
    date_str = context["ds"]

    response = requests.get(
        f"{EVENT_BASE_URL}/events/clickstream",
        params={"batch_size": 100}
    )
    response.raise_for_status()

    data = response.json()["data"]

    local_dir = f"{TEMP_BASE}/events/clickstream/{date_str}"
    ensure_path(local_dir)

    local_file = f"{local_dir}/clickstream.json"

    with open(local_file, "w") as f:
        json.dump(data, f)

    blob_path = f"events/clickstream/{date_str}/clickstream.json"
    upload_to_adls(local_file, blob_path)

    os.remove(local_file)

    print(f"[SUCCESS] Clickstream events processed")


def fetch_footfall(**context):
    date_str = context["ds"]

    response = requests.get(
        f"{EVENT_BASE_URL}/events/footfall",
        params={"batch_size": 100}
    )
    response.raise_for_status()

    data = response.json()["data"]

    local_dir = f"{TEMP_BASE}/events/footfall/{date_str}"
    ensure_path(local_dir)

    local_file = f"{local_dir}/footfall.json"

    with open(local_file, "w") as f:
        json.dump(data, f)

    blob_path = f"events/footfall/{date_str}/footfall.json"
    upload_to_adls(local_file, blob_path)

    os.remove(local_file)

    print(f"[SUCCESS] Footfall events processed")


def validate_event_outputs(**context):
    # Validation here assumes upload succeeded (Blob overwrite=True)
    print("[VALIDATION] Event files uploaded to ADLS successfully")


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

    [
        fetch_order_events_task,
        fetch_clickstream_task,
        fetch_footfall_task
    ] >> validate_task >> notify_task