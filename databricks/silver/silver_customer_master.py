# Databricks notebook source
# MAGIC %md # Silver — customer_master

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
BRONZE_PATH = f"{BASE}/bronze/customer_master"
SILVER_PATH = f"{BASE}/silver/customer_master"

spark.read.format("delta").load(BRONZE_PATH).createOrReplaceTempView("bronze_customer_master")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE delta.`{SILVER_PATH}`
USING DELTA
PARTITIONED BY (_ingestion_date)
AS
SELECT customer_id, first_name, last_name, email, phone_number, gender, birth_date,
    CAST(registration_date AS DATE) AS registration_date,
    LOWER(TRIM(country)) AS country,
    LOWER(TRIM(loyalty_tier)) AS loyalty_tier,
    _ingested_at, _ingestion_date,
    current_timestamp() AS _updated_at
FROM bronze_customer_master
WHERE customer_id IS NOT NULL AND first_name IS NOT NULL AND last_name IS NOT NULL AND registration_date IS NOT NULL
""")

# COMMAND ----------
count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"silver_customer_master row count: {count}")
