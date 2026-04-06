# Databricks notebook source
# MAGIC %md # Bronze — sales_transactions
# MAGIC Reads raw CSV from ADLS and writes to Delta (append-only, partitioned by ingestion_date)

# COMMAND ----------
# Cell 1 — ADLS Configuration
client_id     = dbutils.secrets.get("retail-analytics", "adls-client-id")
tenant_id     = dbutils.secrets.get("retail-analytics", "adls-tenant-id")
client_secret = dbutils.secrets.get("retail-analytics", "adls-client-secret")
account_name  = dbutils.secrets.get("retail-analytics", "adls-account-name")

spark.conf.set(f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net",
               "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net",     client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net", client_secret)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net",
               f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
print(f"Connected to: {account_name}")

# COMMAND ----------
# Cell 2 — Paths
RAW_PATH    = f"abfss://raw@{account_name}.dfs.core.windows.net/sales_transactions"
BRONZE_PATH = f"abfss://curated@{account_name}.dfs.core.windows.net/bronze/sales_transactions"

# COMMAND ----------
# Cell 3 — Read raw CSV
from pyspark.sql.functions import current_timestamp, to_date

df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)
print(f"Raw row count: {df.count()}")
df.printSchema()

# COMMAND ----------
# Cell 4 — Add audit columns
df = df.withColumn("_ingested_at", current_timestamp()) \
       .withColumn("_ingestion_date", to_date(current_timestamp()))

# COMMAND ----------
# Cell 5 — Write to Bronze Delta (MERGE for idempotency)
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    DeltaTable.forPath(spark, BRONZE_PATH).alias("target").merge(
        df.alias("source"), "target.transaction_id = source.transaction_id"
    ).whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").partitionBy("_ingestion_date").save(BRONZE_PATH)

print("Bronze write complete")

# COMMAND ----------
# Cell 6 — Verify
count = spark.read.format("delta").load(BRONZE_PATH).count()
print(f"bronze_sales_transactions row count: {count}")
