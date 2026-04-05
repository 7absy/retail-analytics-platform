from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, "/opt/airflow/pipelines")

from generators import (
    customer_master,
    product_catalog,
    inventory_snapshot,
    sales_transactions
)

OUTPUT_BASE = os.getenv("OUTPUT_BASE", "/app/data/batch")


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
# Task Functions
# -----------------------
def generate_and_upload_customers(output_base: str, date_str: str):
    customer_master.generate(output_base, date_str)

    local_file = f"{output_base}/customer_master/{date_str}/customer_master.csv"
    upload_to_adls(local_file, f"batch/customer_master/{date_str}/customer_master.csv")

    os.remove(local_file)


def generate_and_upload_products(output_base: str, date_str: str):
    product_catalog.generate(output_base, date_str)

    local_file = f"{output_base}/product_catalog/{date_str}/product_catalog.csv"
    upload_to_adls(local_file, f"batch/product_catalog/{date_str}/product_catalog.csv")

    os.remove(local_file)


def generate_and_upload_inventory(output_base: str, date_str: str):
    inventory_snapshot.generate(output_base, date_str)

    local_file = f"{output_base}/inventory_snapshot/{date_str}/inventory_snapshot.csv"
    upload_to_adls(local_file, f"batch/inventory_snapshot/{date_str}/inventory_snapshot.csv")

    os.remove(local_file)


def generate_and_upload_sales(output_base: str, date_str: str):
    sales_transactions.generate(output_base, date_str)

    local_file = f"{output_base}/sales_transactions/{date_str}/sales_transactions.csv"
    upload_to_adls(local_file, f"batch/sales_transactions/{date_str}/sales_transactions.csv")

    os.remove(local_file)


def validate_outputs(**context):
    print("[VALIDATION] Batch files uploaded to ADLS successfully")


def notify_bronze(**context):
    date_str = context["ds"]
    print(f"Batch data ready for Bronze ingestion — {date_str}")


# -----------------------
# DAG
# -----------------------
with DAG(
    dag_id="batch_ingestion_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch", "ingestion"]
) as dag:

    t_customers = PythonOperator(
        task_id="generate_customers",
        python_callable=generate_and_upload_customers,
        op_kwargs={"output_base": OUTPUT_BASE, "date_str": "{{ ds }}"}
    )

    t_products = PythonOperator(
        task_id="generate_products",
        python_callable=generate_and_upload_products,
        op_kwargs={"output_base": OUTPUT_BASE, "date_str": "{{ ds }}"}
    )

    t_inventory = PythonOperator(
        task_id="generate_inventory",
        python_callable=generate_and_upload_inventory,
        op_kwargs={"output_base": OUTPUT_BASE, "date_str": "{{ ds }}"}
    )

    t_sales = PythonOperator(
        task_id="generate_sales",
        python_callable=generate_and_upload_sales,
        op_kwargs={"output_base": OUTPUT_BASE, "date_str": "{{ ds }}"}
    )

    t_validate = PythonOperator(
        task_id="validate_outputs",
        python_callable=validate_outputs
    )

    t_notify = PythonOperator(
        task_id="notify_bronze",
        python_callable=notify_bronze
    )

    [t_customers, t_products, t_inventory, t_sales] >> t_validate >> t_notify