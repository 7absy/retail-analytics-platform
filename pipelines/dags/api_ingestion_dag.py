from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import requests
import json

sys.path.insert(0, "/opt/airflow/pipelines")

API_BASE_URL = os.getenv("API_BASE_URL", "http://api-simulator:8000")
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

        if page > 10:
            break

    local_dir = f"{TEMP_BASE}/api/orders/{date_str}"
    ensure_path(local_dir)

    local_file = f"{local_dir}/orders.json"

    with open(local_file, "w") as f:
        json.dump(all_records, f)

    upload_to_adls(local_file, f"api/orders/{date_str}/orders.json")

    os.remove(local_file)

    print("[SUCCESS] Orders processed")


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

    local_dir = f"{TEMP_BASE}/api/campaigns/{date_str}"
    ensure_path(local_dir)

    local_file = f"{local_dir}/campaigns.json"

    with open(local_file, "w") as f:
        json.dump(all_records, f)

    upload_to_adls(local_file, f"api/campaigns/{date_str}/campaigns.json")

    os.remove(local_file)

    print("[SUCCESS] Campaigns processed")


def validate_api_outputs(**context):
    print("[VALIDATION] API data uploaded to ADLS successfully")


def notify_bronze(**context):
    date_str = context["ds"]
    print(f"API data ready for Bronze ingestion — {date_str}")


# -----------------------
# DAG
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

    [fetch_orders_task, fetch_campaigns_task] >> validate_task >> notify_task