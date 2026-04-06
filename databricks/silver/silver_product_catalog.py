# Databricks notebook source
# MAGIC %md # Silver — product_catalog

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
BRONZE_PATH = f"{BASE}/bronze/product_catalog"
SILVER_PATH = f"{BASE}/silver/product_catalog"

spark.read.format("delta").load(BRONZE_PATH).createOrReplaceTempView("bronze_product_catalog")

# COMMAND ----------
spark.sql(f"""
CREATE OR REPLACE TABLE delta.`{SILVER_PATH}`
USING DELTA
PARTITIONED BY (_ingestion_date)
AS
SELECT product_id, product_name,
    LOWER(TRIM(category)) AS category,
    LOWER(TRIM(subcategory)) AS subcategory,
    brand,
    CAST(price AS DOUBLE) AS price,
    CAST(cost AS DOUBLE) AS cost,
    launch_date, is_active,
    _ingested_at, _ingestion_date,
    current_timestamp() AS _updated_at
FROM bronze_product_catalog
WHERE product_id IS NOT NULL AND product_name IS NOT NULL AND category IS NOT NULL AND price IS NOT NULL AND is_active IS NOT NULL
""")

# COMMAND ----------
count = spark.read.format("delta").load(SILVER_PATH).count()
print(f"silver_product_catalog row count: {count}")
