# Databricks notebook source
# MAGIC %md # Bronze — orders (API JSON)

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
import datetime
DATE_STR    = datetime.date.today().strftime("%Y-%m-%d")
RAW_PATH    = f"abfss://raw@{account_name}.dfs.core.windows.net/api/orders/{DATE_STR}/orders.json"
BRONZE_PATH = f"abfss://curated@{account_name}.dfs.core.windows.net/bronze/orders"

# COMMAND ----------
from pyspark.sql.functions import current_timestamp, to_date

df = spark.read.option("multiLine", "true").json(RAW_PATH)
df = df.withColumn("_ingested_at", current_timestamp()) \
       .withColumn("_ingestion_date", to_date(current_timestamp()))
print(f"Raw row count: {df.count()}")

# COMMAND ----------
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, BRONZE_PATH):
    DeltaTable.forPath(spark, BRONZE_PATH).alias("target").merge(
        df.alias("source"), "target.order_id = source.order_id"
    ).whenNotMatchedInsertAll().execute()
else:
    df.write.format("delta").mode("overwrite").partitionBy("_ingestion_date").save(BRONZE_PATH)

# COMMAND ----------
count = spark.read.format("delta").load(BRONZE_PATH).count()
print(f"bronze_orders row count: {count}")
