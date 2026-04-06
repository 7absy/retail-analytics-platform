# Databricks notebook source
# MAGIC %md # Silver — order_events

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
BRONZE_PATH = f"{BASE}/bronze/order_events"
SILVER_PATH = f"{BASE}/silver/order_events"

spark.read.format("delta").load(BRONZE_PATH).createOrReplaceTempView("bronze_order_events")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE delta.`{SILVER_PATH}`
USING DELTA
PARTITIONED BY (_ingestion_date)
AS
SELECT event_id,
    UPPER(TRIM(event_type)) AS event_type,
    CAST(occurred_at AS TIMESTAMP) AS occurred_at,
    order_id, customer_id,
    UPPER(TRIM(status)) AS status,
    warehouse_id, carrier,
    _ingested_at, _ingestion_date,
    current_timestamp() AS _updated_at
FROM bronze_order_events
WHERE event_id IS NOT NULL AND event_type IS NOT NULL AND occurred_at IS NOT NULL AND order_id IS NOT NULL AND customer_id IS NOT NULL
""")

# COMMAND ----------
count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"silver_order_events row count: {count}")
