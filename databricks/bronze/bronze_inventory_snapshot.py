# Databricks notebook source
# MAGIC %md # Bronze — inventory_snapshot

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

# COMMAND ----------
RAW_PATH    = f"abfss://raw@{account_name}.dfs.core.windows.net/inventory_snapshot"
BRONZE_PATH = f"abfss://curated@{account_name}.dfs.core.windows.net/bronze/inventory_snapshot"

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, to_date

df = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_PATH)
print(f"Raw row count: {df.count()}")

# COMMAND ----------
df = df.withColumn("_ingested_at", current_timestamp()) \
       .withColumn("_ingestion_date", to_date(current_timestamp()))

# COMMAND ----------
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    DeltaTable.forPath(spark, BRONZE_PATH).alias("target").merge(
        df.alias("source"),
        "target.product_id = source.product_id AND target.store_id = source.store_id AND target.snapshot_date = source.snapshot_date"
    ).whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").partitionBy("_ingestion_date").save(BRONZE_PATH)

# COMMAND ----------
count = spark.read.format("delta").load(BRONZE_PATH).count()
print(f"bronze_inventory_snapshot row count: {count}")
