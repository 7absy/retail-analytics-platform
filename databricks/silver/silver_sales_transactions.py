# Databricks notebook source
# MAGIC %md # Silver — sales_transactions

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
BRONZE_PATH = f"{BASE}/bronze/sales_transactions"
SILVER_PATH = f"{BASE}/silver/sales_transactions"

spark.read.format("delta").load(BRONZE_PATH).createOrReplaceTempView("bronze_sales_transactions")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE delta.`{SILVER_PATH}`
USING DELTA
PARTITIONED BY (_ingestion_date)
AS
SELECT transaction_id, order_id, customer_id, product_id,
    CAST(quantity AS INT) AS quantity,
    CAST(unit_price AS DOUBLE) AS unit_price,
    CAST(total_price AS DOUBLE) AS total_price,
    LOWER(TRIM(channel)) AS channel,
    store_id,
    CAST(transaction_date AS TIMESTAMP) AS transaction_date,
    _ingested_at, _ingestion_date,
    current_timestamp() AS _updated_at
FROM bronze_sales_transactions
WHERE transaction_id IS NOT NULL AND product_id IS NOT NULL AND quantity IS NOT NULL AND unit_price IS NOT NULL AND channel IS NOT NULL
""")

# COMMAND ----------
count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"silver_sales_transactions row count: {count}")
