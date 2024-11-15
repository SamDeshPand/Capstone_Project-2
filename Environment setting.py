# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

def use_catalog():
  spark.sql("use catalog capstone_project2")

# COMMAND ----------

def get_storage_location():
  return "dbfs:/mnt/adlssonydatabricks/raw/project2/"

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/adlssonydatabricks/raw/project2/qualifying/qualifying

# COMMAND ----------

def add_ingestion_date(df):
  df1 = df.withColumn("ingestion_date", current_date())
  return df1

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

def process_and_write_stream(df, name):
    df_with_ingestion_date = df.withColumn("ingestion_date", current_timestamp())
    df_non_null = df_with_ingestion_date.dropna(how='all')
    (df_non_null.writeStream
     .format("delta")
     .option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_{name}")
     .outputMode("append")
     .trigger(availableNow=True)
     .table(f"capstone_project2.bronze.{name}"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from json.`dbfs:/mnt/adlssonydatabricks/raw/project2/qualifying`
