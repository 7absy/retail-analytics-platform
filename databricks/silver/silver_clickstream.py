# Databricks notebook source
# MAGIC %md # Silver — clickstream

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
BRONZE_PATH = f"{BASE}/bronze/clickstream"
SILVER_PATH = f"{BASE}/silver/clickstream"

spark.read.format("delta").load(BRONZE_PATH).createOrReplaceTempView("bronze_clickstream")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE delta.`{SILVER_PATH}`
USING DELTA
PARTITIONED BY (_ingestion_date)
AS
SELECT event_id,
    UPPER(TRIM(event_type)) AS event_type,
    CAST(occurred_at AS TIMESTAMP) AS occurred_at,
    user_id, session_id, page_url, product_id,
    LOWER(TRIM(device_type)) AS device_type,
    geo_location,
    _ingested_at, _ingestion_date,
    current_timestamp() AS _updated_at
FROM bronze_clickstream
WHERE event_id IS NOT NULL AND event_type IS NOT NULL AND occurred_at IS NOT NULL AND session_id IS NOT NULL AND page_url IS NOT NULL
""")

# COMMAND ----------
count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"silver_clickstream row count: {count}")
