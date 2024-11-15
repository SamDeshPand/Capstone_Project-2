# Databricks notebook source
# MAGIC
# MAGIC %run "/Workspace/Users/sameerdeshpande51@gmail.com/Capstone_Project-2/Environment setting"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from csv.`dbfs:/mnt/adlssonydatabricks/raw/project2/races/races.csv`

# COMMAND ----------

from pyspark.sql.functions import current_timestamp


# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/checkpoints2_driver", True)

# COMMAND ----------

# MAGIC %md
# MAGIC Driver Data ingestion

# COMMAND ----------

df_driver = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2driver")
      .option("cloudFiles.inferColumnTypes","true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/drivers/"))
df_driver_with_ingestion_date = df_driver.withColumn("ingestion_date", current_timestamp())
df_driver_non_null = df_driver_with_ingestion_date.dropna(how='all')
(df_driver_non_null.writeStream
.format("delta")
.option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_driver")
.outputMode("append")
.trigger(availableNow=True)
.table(f"capstone_project2.bronze.driver"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.bronze.driver

# COMMAND ----------

df_circuits = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2circuits")
      .option("header","true")
      .option("cloudFiles.inferColumnTypes","true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/circuits/"))
df_circuits_with_ingestion_date = df_circuits.withColumn("ingestion_date", current_timestamp())
df_circuits_non_null = df_circuits_with_ingestion_date.dropna(how='all')
(df_circuits_non_null.writeStream
.format("delta")
.option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_circuits")
.outputMode("append")
.trigger(availableNow=True)
.table(f"capstone_project2.bronze.circuits"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.bronze.circuits

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit, to_timestamp

df_races = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2races")
      .option("header","true")
      .option("cloudFiles.inferColumnTypes","true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/races/"))

df_races = df_races.withColumn("timestamp", to_timestamp(concat(col("date"), lit(" "), col("time"))))

df_races_with_ingestion_date = df_races.withColumn("ingestion_date", current_timestamp())
df_races_non_null = df_races_with_ingestion_date.dropna(how='all')
(df_races_non_null.writeStream
.format("delta")
.option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_races")
.outputMode("append")
.trigger(availableNow=True)
.table(f"capstone_project2.bronze.races"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.bronze.races

# COMMAND ----------

df_cons = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2constructors")
      .option("cloudFiles.inferColumnTypes","true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/constructors/"))

df_cons_with_ingestion_date = df_cons.withColumn("ingestion_date", current_timestamp())
df_cons_non_null = df_cons_with_ingestion_date.dropna(how='all')
(df_cons_non_null.writeStream
.format("delta")
.option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_constructors")
.outputMode("append")
.trigger(availableNow=True)
.table(f"capstone_project2.bronze.constructors"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.bronze.constructors

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

df_results = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2results")
      .option("cloudFiles.inferColumnTypes","true")
      .option("cloudFiles.schemaHints", "fastestLap INTEGER, fastestLapSpeed DOUBLE, fastestLapTime STRING, milliseconds INTEGER")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/results/"))

df_results.display()
df_results_with_ingestion_date = df_results.withColumn("ingestion_date", current_timestamp())
df_results_non_null = df_results_with_ingestion_date.dropna(how='all')
(df_results_non_null.writeStream
.format("delta")
.option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_results")
.outputMode("append")
.trigger(availableNow=True)
.table(f"capstone_project2.bronze.results"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.bronze.results

# COMMAND ----------

df_pitstops = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2pitstops")
      .option("cloudFiles.inferColumnTypes","true")
      .option("cloudFiles.schemaHints", "duration DOUBLE, time TIMESTAMP")
      .option("multiline", "true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/pitstops/"))
df_pitstops_with_ingestion_date = df_pitstops.withColumn("ingestion_date", current_timestamp())
df_pitstops_non_null = df_pitstops_with_ingestion_date.dropna(how='all')
(df_pitstops_non_null.writeStream
.format("delta")
.option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_pitstops")
.outputMode("append")
.trigger(availableNow=True)
.table(f"capstone_project2.bronze.pitstops"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.bronze.pitstops

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.bronze.races

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType, StringType

schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", DoubleType(), True)
])

df_laptime = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2laptimes")
      .schema(schema)
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/laptimes/lap_times/"))
df_laptime_with_ingestion_date = df_laptime.withColumn("ingestion_date", current_timestamp())
df_laptime_non_null = df_laptime_with_ingestion_date.dropna(how='all')
(df_laptime_non_null.writeStream
.format("delta")
.option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_laptimes")
.outputMode("append")
.trigger(AvailableNow=True)
.table(f"capstone_project2.bronze.laptimes"))

# COMMAND ----------


process_and_write_stream(df_laptime, "laptimes")

# COMMAND ----------

df_qualifying = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2qualifying")
      .option("multiline", "true")
      .option("cloudFiles.inferColumnTypes", "true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/qualifying/qualifying"))
df_qualifying_with_ingestion_date = df_qualifying.withColumn("ingestion_date", current_timestamp())
df_qualifying_non_null = df_qualifying_with_ingestion_date.dropna(how='all')
(df_qualifying_non_null.writeStream
.format("delta")
.option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_qualifying")
.outputMode("append")
.trigger(availableNow=True)
.table(f"capstone_project2.bronze.qualifying"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.bronze.qualifying
