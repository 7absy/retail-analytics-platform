# Databricks notebook source
# MAGIC %md # Gold — dim_campaign

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
spark.sql(f"""
CREATE OR REPLACE TABLE delta.`{BASE}/gold/dim_campaign`
USING DELTA AS
SELECT
    ROW_NUMBER() OVER (ORDER BY campaign_id) AS campaign_key,
    campaign_id, campaign_name, channel,
    CAST(start_date AS DATE) AS start_date,
    CAST(end_date   AS DATE) AS end_date,
    CAST(budget AS DOUBLE)   AS budget,
    target_audience, status,
    current_timestamp()      AS _updated_at
FROM delta.`{BASE}/silver/campaigns`
""")

count = spark.read.format("delta").load(f"{BASE}/gold/dim_campaign").count()
print(f"dim_campaign row count: {count}")
