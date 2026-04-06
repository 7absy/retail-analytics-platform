# Databricks notebook source
# MAGIC %md # Gold — fact_inventory
# MAGIC Daily inventory snapshots with reorder flag and stock availability.

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
CREATE OR REPLACE TABLE delta.`{BASE}/gold/fact_inventory`
USING DELTA
PARTITIONED BY (snapshot_year, snapshot_month)
AS
SELECT
    p.product_key,
    st.store_key,
    CAST(date_format(i.snapshot_date, 'yyyyMMdd') AS INT) AS date_key,
    i.stock_on_hand,
    i.stock_reserved,
    COALESCE(i.stock_on_hand, 0) - COALESCE(i.stock_reserved, 0) AS stock_available,
    i.reorder_level,
    CASE WHEN i.stock_on_hand <= i.reorder_level THEN true ELSE false END AS needs_reorder,
    i.warehouse_id,
    YEAR(i.snapshot_date)  AS snapshot_year,
    MONTH(i.snapshot_date) AS snapshot_month,
    current_timestamp()    AS _updated_at
FROM delta.`{BASE}/silver/inventory_snapshot` i
LEFT JOIN delta.`{BASE}/gold/dim_product` p  ON i.product_id = p.product_id
LEFT JOIN delta.`{BASE}/gold/dim_store`   st ON i.store_id   = st.store_id
""")

count = spark.read.format("delta").load(f"{BASE}/gold/fact_inventory").count()
print(f"fact_inventory row count: {count}")

spark.sql(f"""
SELECT COUNT(*) AS total_snapshots, SUM(stock_on_hand) AS total_stock,
    SUM(stock_available) AS total_available,
    SUM(CASE WHEN needs_reorder THEN 1 ELSE 0 END) AS items_needing_reorder
FROM delta.`{BASE}/gold/fact_inventory`
""").show()
