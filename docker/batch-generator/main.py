import os
from datetime import date

from generators import (
    customer_master,
    product_catalog,
    inventory_snapshot,
    sales_transactions  # FIXED: added missing generator
)

OUTPUT_BASE = os.getenv("OUTPUT_BASE", "/app/data/batch")
TODAY = date.today().isoformat()


def log(msg: str):
    print(f"[BATCH-GENERATOR] {msg}")


if __name__ == "__main__":

    log("Starting batch generation job")
    log(f"Output base: {OUTPUT_BASE}")
    log(f"Date: {TODAY}")

    try:
        log("Generating customer_master...")
        customer_master.generate(OUTPUT_BASE, TODAY)
        log("customer_master completed")

        log("Generating product_catalog...")
        product_catalog.generate(OUTPUT_BASE, TODAY)
        log("product_catalog completed")

        log("Generating inventory_snapshot...")
        inventory_snapshot.generate(OUTPUT_BASE, TODAY)
        log("inventory_snapshot completed")

        # NEW STEP
        log("Generating sales_transactions...")
        sales_transactions.generate(OUTPUT_BASE, TODAY)
        log("sales_transactions completed")

        log("ALL BATCH JOBS COMPLETED SUCCESSFULLY")

    except Exception as e:
        log(f"FAILED: {str(e)}")
        raise