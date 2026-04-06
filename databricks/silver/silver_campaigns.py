# Databricks notebook source
# MAGIC %md # Silver — campaigns

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
BRONZE_PATH = f"{BASE}/bronze/campaigns"
SILVER_PATH = f"{BASE}/silver/campaigns"

spark.read.format("delta").load(BRONZE_PATH).createOrReplaceTempView("bronze_campaigns")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE delta.`{SILVER_PATH}`
USING DELTA
PARTITIONED BY (_ingestion_date)
AS
SELECT campaign_id, campaign_name,
    LOWER(TRIM(channel)) AS channel,
    CAST(start_date AS DATE) AS start_date,
    CAST(end_date AS DATE) AS end_date,
    CAST(budget AS DOUBLE) AS budget,
    target_audience,
    UPPER(TRIM(status)) AS status,
    _ingested_at, _ingestion_date,
    current_timestamp() AS _updated_at
FROM bronze_campaigns
WHERE campaign_id IS NOT NULL AND campaign_name IS NOT NULL AND channel IS NOT NULL AND start_date IS NOT NULL AND status IS NOT NULL
""")

# COMMAND ----------
count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"silver_campaigns row count: {count}")
