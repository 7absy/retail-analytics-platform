# Databricks notebook source
# MAGIC %md # Silver — inventory_snapshot

# COMMAND ----------
client_id     = dbutils.secrets.get("retail-analytics", "adls-client-id")
tenant_id     = dbutils.secrets.get("retail-analytics", "adls-tenant-id")
client_secret = dbutils.secrets.get("retail-analytics", "adls-client-secret")
account_name  = dbutils.secrets.get("retail-analytics", "adls-account-name")

spark.conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

BASE = f"abfss://curated@{account_name}.dfs.core.windows.net"

# COMMAND ----------
BRONZE_PATH = f"{BASE}/bronze/inventory_snapshot"
SILVER_PATH = f"{BASE}/silver/inventory_snapshot"

spark.read.format("delta").load(BRONZE_PATH).createOrReplaceTempView("bronze_inventory_snapshot")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE delta.`{SILVER_PATH}`
USING DELTA
PARTITIONED BY (_ingestion_date)
AS
SELECT CAST(snapshot_date AS DATE) AS snapshot_date,
    product_id, store_id, warehouse_id,
    CAST(stock_on_hand AS INT) AS stock_on_hand,
    CAST(stock_reserved AS INT) AS stock_reserved,
    CAST(reorder_level AS INT) AS reorder_level,
    _ingested_at, _ingestion_date,
    current_timestamp() AS _updated_at
FROM bronze_inventory_snapshot
WHERE snapshot_date IS NOT NULL AND product_id IS NOT NULL AND store_id IS NOT NULL AND stock_on_hand IS NOT NULL
""")

# COMMAND ----------
count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"silver_inventory_snapshot row count: {count}")
