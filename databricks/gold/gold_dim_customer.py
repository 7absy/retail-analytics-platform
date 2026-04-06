# Databricks notebook source
# MAGIC %md # Gold — dim_customer

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
CREATE OR REPLACE TABLE delta.`{BASE}/gold/dim_customer`
USING DELTA AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    customer_id,
    first_name, last_name,
    CONCAT(first_name, ' ', last_name)       AS full_name,
    email, phone_number, gender, birth_date,
    country, loyalty_tier,
    CAST(registration_date AS DATE)          AS registration_date,
    current_timestamp()                      AS _updated_at
FROM delta.`{BASE}/silver/customer_master`
""")

count = spark.read.format("delta").load(f"{BASE}/gold/dim_customer").count()
print(f"dim_customer row count: {count}")
