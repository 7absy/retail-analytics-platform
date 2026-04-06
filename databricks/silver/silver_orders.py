# Databricks notebook source
# MAGIC %md # Silver — orders

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
BRONZE_PATH = f"{BASE}/bronze/orders"
SILVER_PATH = f"{BASE}/silver/orders"

spark.read.format("delta").load(BRONZE_PATH).createOrReplaceTempView("bronze_orders")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE delta.`{SILVER_PATH}`
USING DELTA
PARTITIONED BY (_ingestion_date)
AS
SELECT order_id, customer_id,
    UPPER(TRIM(order_status)) AS order_status,
    CAST(order_date AS TIMESTAMP) AS order_date,
    LOWER(TRIM(channel)) AS channel,
    CAST(total_amount AS DOUBLE) AS total_amount,
    currency,
    CAST(discount_amount AS DOUBLE) AS discount_amount,
    CAST(shipping_cost AS DOUBLE) AS shipping_cost,
    payment_method, shipping_address,
    _ingested_at, _ingestion_date,
    current_timestamp() AS _updated_at
FROM bronze_orders
WHERE order_id IS NOT NULL AND customer_id IS NOT NULL AND order_status IS NOT NULL AND order_date IS NOT NULL AND channel IS NOT NULL AND total_amount IS NOT NULL
""")

# COMMAND ----------
count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"silver_orders row count: {count}")
