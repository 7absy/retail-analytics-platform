# Databricks notebook source
# MAGIC %md # Gold — dim_date
# MAGIC Generated date spine 2020–2030. No source file required.

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
CREATE OR REPLACE TABLE delta.`{BASE}/gold/dim_date`
USING DELTA AS
SELECT
    CAST(date_format(date, 'yyyyMMdd') AS INT) AS date_key,
    date                                        AS full_date,
    YEAR(date)                                  AS year,
    MONTH(date)                                 AS month,
    DAY(date)                                   AS day,
    QUARTER(date)                               AS quarter,
    DATE_FORMAT(date, 'EEEE')                   AS day_name,
    DATE_FORMAT(date, 'MMMM')                   AS month_name,
    WEEKOFYEAR(date)                            AS week_of_year,
    CASE WHEN DATE_FORMAT(date, 'E') IN ('Sat','Sun') THEN true ELSE false END AS is_weekend
FROM (SELECT EXPLODE(SEQUENCE(TO_DATE('2020-01-01'), TO_DATE('2030-12-31'), INTERVAL 1 DAY)) AS date)
""")

count = spark.read.format("delta").load(f"{BASE}/gold/dim_date").count()
print(f"dim_date row count: {count}")
