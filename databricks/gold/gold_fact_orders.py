# Databricks notebook source
# MAGIC %md # Gold — fact_orders
# MAGIC Order-level fact table from API source. Joins to customer, campaign, date dimensions.

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
CREATE OR REPLACE TABLE delta.`{BASE}/gold/fact_orders`
USING DELTA
PARTITIONED BY (order_year, order_month)
AS
SELECT
    o.order_id,
    c.customer_key,
    CAST(date_format(o.order_date, 'yyyyMMdd') AS INT) AS date_key,
    o.order_status,
    o.channel,
    o.total_amount,
    o.discount_amount,
    o.shipping_cost,
    ROUND(o.total_amount - COALESCE(o.discount_amount, 0) - COALESCE(o.shipping_cost, 0), 2) AS net_revenue,
    o.payment_method,
    o.currency,
    YEAR(o.order_date)  AS order_year,
    MONTH(o.order_date) AS order_month,
    current_timestamp() AS _updated_at
FROM delta.`{BASE}/silver/orders` o
LEFT JOIN delta.`{BASE}/gold/dim_customer` c ON o.customer_id = c.customer_id
""")

count = spark.read.format("delta").load(f"{BASE}/gold/fact_orders").count()
print(f"fact_orders row count: {count}")
