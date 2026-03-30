from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Add project root to path
sys.path.insert(0, "/app")

from generators import (
    customer_master,
    product_catalog,
    inventory_snapshot,
    sales_transactions
)

OUTPUT_BASE = os.getenv("OUTPUT_BASE", "/app/data/batch")


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
def generate_customers(output_base: str, date_str: str):
    customer_master.generate(output_base, date_str)


def generate_products(output_base: str, date_str: str):
    product_catalog.generate(output_base, date_str)


def generate_inventory(output_base: str, date_str: str):
    inventory_snapshot.generate(output_base, date_str)


def generate_sales(output_base: str, date_str: str):
    sales_transactions.generate(output_base, date_str)


def validate_outputs(output_base: str, date_str: str):
    required_files = [
        f"{output_base}/customer_master/{date_str}/customer_master.csv",
        f"{output_base}/product_catalog/{date_str}/product_catalog.csv",
        f"{output_base}/inventory_snapshot/{date_str}/inventory_snapshot.csv",
        f"{output_base}/sales_transactions/{date_str}/sales_transactions.csv",
    ]

    missing = [f for f in required_files if not os.path.exists(f)]

    if missing:
        raise FileNotFoundError(f"Missing batch files: {missing}")


def trigger_bronze_notification(date_str: str):
    print(f"Batch files ready for Bronze ingestion — {date_str}")


# -----------------------
# DAG Definition
# -----------------------
with DAG(
    dag_id="batch_ingestion_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch", "ingestion"]
) as dag:

    generate_customers_task = PythonOperator(
        task_id="generate_customers",
        python_callable=generate_customers,
        op_kwargs={
            "output_base": OUTPUT_BASE,
            "date_str": "{{ ds }}"
        }
    )

    generate_products_task = PythonOperator(
        task_id="generate_products",
        python_callable=generate_products,
        op_kwargs={
            "output_base": OUTPUT_BASE,
            "date_str": "{{ ds }}"
        }
    )

    generate_inventory_task = PythonOperator(
        task_id="generate_inventory",
        python_callable=generate_inventory,
        op_kwargs={
            "output_base": OUTPUT_BASE,
            "date_str": "{{ ds }}"
        }
    )

    generate_sales_task = PythonOperator(
        task_id="generate_sales",
        python_callable=generate_sales,
        op_kwargs={
            "output_base": OUTPUT_BASE,
            "date_str": "{{ ds }}"
        }
    )

    validate_outputs_task = PythonOperator(
        task_id="validate_outputs",
        python_callable=validate_outputs,
        op_kwargs={
            "output_base": OUTPUT_BASE,
            "date_str": "{{ ds }}"
        }
    )

    trigger_bronze_notification_task = PythonOperator(
        task_id="trigger_bronze_notification",
        python_callable=trigger_bronze_notification,
        op_kwargs={
            "date_str": "{{ ds }}"
        }
    )

    # -----------------------
    # FIX 2: Parallel → Validate → Notify
    # -----------------------
    [
        generate_customers_task,
        generate_products_task,
        generate_inventory_task,
        generate_sales_task,
    ] >> validate_outputs_task >> trigger_bronze_notification_task