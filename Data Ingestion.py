# Databricks notebook source
# MAGIC
# MAGIC %run "/Workspace/Users/sameerdeshpande51@gmail.com/Capstone_Project-2/Environment setting"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
def process_and_write_stream(df, name):
    df_with_ingestion_date = df.withColumn("ingestion_date", current_timestamp())
    df_non_null = df_with_ingestion_date.dropna(how='all')
    (df_non_null.writeStream
     .format("delta")
     .option("checkpointLocation", f"dbfs:/FileStore/checkpoints2_{name}")
     .outputMode("append")
     .trigger(AvailableNow=True)
     .table(f"capstone_project2.bronze.{name}"))

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
display(df_driver)

# COMMAND ----------

process_and_write_stream(df_driver, "driver")

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
display(df_circuits)

# COMMAND ----------

process_and_write_stream(df_circuits, "circuits")

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

display(df_races)

# COMMAND ----------

process_and_write_stream(df_races, "races")

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
display(df_cons)

# COMMAND ----------

process_and_write_stream(df_cons, "constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.bronze.constructors

# COMMAND ----------

from pyspark.sql.functions import col

df_results = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2results")
      .option("cloudFiles.inferColumnTypes","true")
      .option("cloudFiles.schemaHints", "fastestLap INTEGER, fastestLapSpeed DOUBLE, fastestLapTime STRING, milliseconds INTEGER")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/results/"))

display(df_results)

# COMMAND ----------

from pyspark.sql.functions import split, expr

df_results1 = df_results.withColumn("fastestLap", col("fastestLap").cast("integer")) \
                       .withColumn("fastestLapSpeed", col("fastestLapSpeed").cast("double")) \
                       .withColumn("fastestLapTime", expr("split(fastestLapTime, ':')[0] * 60 + split(fastestLapTime, ':')[1]").cast("double"))\
                       .withColumn("milliseconds", col("milliseconds").cast("integer"))

display(df_results1)

# COMMAND ----------

df_pitstops = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2pitstops")
      .option("cloudFiles.inferColumnTypes","true")
      .option("cloudFiles.schemaHints", "duration DOUBLE, time TIMESTAMP")
      .option("multiline", "true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/pitstops/"))
df_pitstops.display()

# COMMAND ----------

process_and_write_stream(df_pitstops, "pitstops")

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
display(df_laptime)

# COMMAND ----------


process_and_write_stream(df_laptime, "laptimes")

# COMMAND ----------

df_qualifying = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.schemaLocation", "dbfs:/FileStore/schemas_capstone2qualifying")
      .option("multiline", "true")
      .load("dbfs:/mnt/adlssonydatabricks/raw/project2/qualifying/qualifying"))
display(df_qualifying)
