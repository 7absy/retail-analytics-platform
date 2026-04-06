# Databricks notebook source
# MAGIC %md # Gold — fact_sales
# MAGIC Core sales fact table. Joins transactions to all 4 dimensions.
# MAGIC Partitioned by year + month for query performance.

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
CREATE OR REPLACE TABLE delta.`{BASE}/gold/fact_sales`
USING DELTA
PARTITIONED BY (transaction_year, transaction_month)
AS
SELECT
    s.transaction_id,
    s.order_id,
    c.customer_key,
    p.product_key,
    st.store_key,
    CAST(date_format(s.transaction_date, 'yyyyMMdd') AS INT) AS date_key,
    s.quantity,
    s.unit_price,
    s.total_price,
    ROUND(s.quantity * (p.price - p.cost), 2)                AS gross_profit,
    s.channel,
    YEAR(s.transaction_date)                                 AS transaction_year,
    MONTH(s.transaction_date)                                AS transaction_month,
    current_timestamp()                                      AS _updated_at
FROM delta.`{BASE}/silver/sales_transactions` s
LEFT JOIN delta.`{BASE}/gold/dim_customer` c  ON s.customer_id = c.customer_id
LEFT JOIN delta.`{BASE}/gold/dim_product`  p  ON s.product_id  = p.product_id
LEFT JOIN delta.`{BASE}/gold/dim_store`    st ON s.store_id    = st.store_id
""")

count = spark.read.format("delta").load(f"{BASE}/gold/fact_sales").count()
print(f"fact_sales row count: {count}")

spark.sql(f"""
SELECT channel,
    COUNT(*) AS total_transactions,
    ROUND(SUM(total_price), 2) AS total_revenue,
    ROUND(SUM(gross_profit), 2) AS total_gross_profit
FROM delta.`{BASE}/gold/fact_sales`
GROUP BY channel ORDER BY total_revenue DESC
""").show()
