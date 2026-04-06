# Databricks notebook source
# MAGIC %md # Gold — fact_footfall
# MAGIC In-store traffic events. Used for store performance and customer flow analysis.

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
CREATE OR REPLACE TABLE delta.`{BASE}/gold/fact_footfall`
USING DELTA
PARTITIONED BY (event_year, event_month)
AS
SELECT
    ff.event_id,
    st.store_key,
    CAST(date_format(ff.occurred_at, 'yyyyMMdd') AS INT) AS date_key,
    ff.event_type,
    ff.customer_type,
    ff.sensor_id,
    YEAR(ff.occurred_at)  AS event_year,
    MONTH(ff.occurred_at) AS event_month,
    current_timestamp()   AS _updated_at
FROM delta.`{BASE}/silver/footfall` ff
LEFT JOIN delta.`{BASE}/gold/dim_store` st ON ff.store_id = st.store_id
""")

count = spark.read.format("delta").load(f"{BASE}/gold/fact_footfall").count()
print(f"fact_footfall row count: {count}")
