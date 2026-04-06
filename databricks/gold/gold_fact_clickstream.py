# Databricks notebook source
# MAGIC %md # Gold — fact_clickstream
# MAGIC Digital engagement events. Used for real-time and batch customer behaviour analysis.

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
CREATE OR REPLACE TABLE delta.`{BASE}/gold/fact_clickstream`
USING DELTA
PARTITIONED BY (event_year, event_month)
AS
SELECT
    cs.event_id,
    cs.session_id,
    p.product_key,
    CAST(date_format(cs.occurred_at, 'yyyyMMdd') AS INT) AS date_key,
    cs.event_type,
    cs.page_url,
    cs.device_type,
    cs.geo_location,
    YEAR(cs.occurred_at)  AS event_year,
    MONTH(cs.occurred_at) AS event_month,
    current_timestamp()   AS _updated_at
FROM delta.`{BASE}/silver/clickstream` cs
LEFT JOIN delta.`{BASE}/gold/dim_product` p ON cs.product_id = p.product_id
""")

count = spark.read.format("delta").load(f"{BASE}/gold/fact_clickstream").count()
print(f"fact_clickstream row count: {count}")
