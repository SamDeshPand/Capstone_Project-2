# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

from pyspark.sql.functions import current_date, to_date

df_driver = spark.table("capstone_project2.bronze.driver").filter(to_date(col("ingestion_date")) == current_date())

df_driver.display()

# COMMAND ----------

from pyspark.sql.functions import col, concat_ws

df_driver_refined=(df_driver.drop("url").drop("_rescued_data")
                  .withColumn("name", concat_ws(" ", col("name.forename"), col("name.surname")))
                  .withColumnRenamed("driverId","driver_id")
                  .withColumnRenamed("driverRef","driver_ref")
                  )
df_driver_refined.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS capstone_project2.silver.driver_scd2 (
# MAGIC   driver_id BIGINT,
# MAGIC   driver_ref STRING,
# MAGIC   name STRING,
# MAGIC   dob STRING,
# MAGIC   nationality STRING,
# MAGIC   number BIGINT,
# MAGIC   ingestion_date TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from delta.tables import DeltaTable

df_driver_refined = df_driver_refined.dropDuplicates().dropna(how="all")
silver_table = DeltaTable.forName(spark, "capstone_project2.silver.driver_scd2")

# Define the merge condition and update/insert actions
merge_condition = "silver.driver_id = bronze.driver_id AND silver.is_current = true"
update_action = {
    "is_current": "false",
    "end_date": "current_timestamp()"
}
insert_action = {
    "driver_id": "bronze.driver_id",
    "driver_ref": "bronze.driver_ref",
    "name": "bronze.name",
    "dob": "bronze.dob",
    "nationality": "bronze.nationality",
    "number": "bronze.number",
    "ingestion_date": "bronze.ingestion_date",
    "is_current": "true",
    "start_date": "current_timestamp()",
    "end_date": "null"
}

# Perform the merge
silver_table.alias("silver").merge(
    df_driver_refined.alias("bronze"),
    merge_condition
).whenMatchedUpdate(
    set=update_action
).whenNotMatchedInsert(
    values=insert_action
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.silver.driver_scd2

# COMMAND ----------

from pyspark.sql.functions import col

df_circuits = spark.read.table("capstone_project2.bronze.circuits").filter(to_date(col("ingestion_date")) == current_date())

df_circuits_refined = (df_circuits.drop("url").drop("_rescued_data")
                       .withColumnRenamed("circuitId", "circuit_id")
                       .withColumnRenamed("circuitRef", "circuit_ref")
                       .withColumnRenamed("lat", "latitude")
                       .withColumnRenamed("lng", "longitude")
                       .withColumnRenamed("alt", "altitude")
                       )

# Filter out rows with invalid latitude and longitude
df_circuits_refined = df_circuits_refined.filter(
    (col("latitude").between(-90, 90)) & 
    (col("longitude").between(-180, 180))
)

df_circuits_refined = df_circuits_refined.dropDuplicates().dropna(how="all")
df_circuits_refined.write.format("delta").mode("append").saveAsTable("capstone_project2.silver.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.silver.circuits

# COMMAND ----------

from pyspark.sql.functions import col

df_races = spark.read.table("capstone_project2.bronze.races").filter(to_date(col("ingestion_date")) == current_date())

df_races_refined = (df_races.drop("_rescued_data").drop("url")
                    .withColumnRenamed("raceId", "race_id")
                    .withColumnRenamed("circuitId", "circuit_id")
                    .withColumnRenamed("year", "race_year")
                    .drop("date")
                    .drop("time")
                    .withColumnRenamed("timestamp", "race_timestamp")
                    )


df_races_refined = df_races_refined.dropDuplicates().dropna(how="all")
df_races_refined.write.format("delta").mode("append").partitionBy("race_year").saveAsTable("capstone_project2.silver.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.silver.races

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS capstone_project2.silver.constructor_scd2 (
# MAGIC   constructor_id BIGINT,
# MAGIC   constructor_ref STRING,
# MAGIC   name STRING,
# MAGIC   nationality STRING,
# MAGIC   ingestion_date TIMESTAMP,
# MAGIC   is_current BOOLEAN,
# MAGIC   start_date TIMESTAMP,
# MAGIC   end_date TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# Load the Bronze table
df_bronze = spark.read.format("delta").table("capstone_project2.bronze.constructors").filter(to_date(col("ingestion_date")) == current_date())

# Prepare the Silver table
silver_table = DeltaTable.forName(spark, "capstone_project2.silver.constructor_scd2")

# Define the merge condition and update/insert actions
merge_condition = "silver.constructor_id = bronze.constructorId AND silver.is_current = true"
update_action = {
    "is_current": "false",
    "end_date": "current_timestamp()"
}
insert_action = {
    "constructor_id": "bronze.constructorId",
    "constructor_ref": "bronze.constructorRef",
    "name": "bronze.name",
    "nationality": "bronze.nationality",
    "ingestion_date": "bronze.ingestion_date",
    "is_current": "true",
    "start_date": "current_timestamp()",
    "end_date": "null"
}

# Perform the merge
silver_table.alias("silver").merge(
    df_bronze.alias("bronze"),
    merge_condition
).whenMatchedUpdate(
    set=update_action
).whenNotMatchedInsert(
    values=insert_action
).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.silver.constructor_scd2

# COMMAND ----------

df_results = spark.read.table("capstone_project2.bronze.results").filter(to_date(col("ingestion_date")) == current_date())

df_results_refined = (df_results.drop("statusId").drop("_rescued_data")
                      .withColumnRenamed("resultId", "result_id")
                      .withColumnRenamed("raceId", "race_id")
                      .withColumnRenamed("driverId", "driver_id")
                      .withColumnRenamed("constructorId", "constructor_id")
                      .withColumnRenamed("positionOrder", "position_order")
                      .withColumnRenamed("fastestLap", "fastest_lap")
                      .withColumnRenamed("fastestLapTime", "fastest_lap_time")
                      .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")
                      .withColumnRenamed("positionText", "position_text")
                      )

df_results_refined = df_results_refined.dropDuplicates().dropna(how="all")
df_results_refined.write.format("delta").mode("append").partitionBy("race_id").saveAsTable("capstone_project2.silver.results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.silver.results
# MAGIC

# COMMAND ----------

# Load the Bronze table for pitstops
df_pitstops = spark.read.table("capstone_project2.bronze.pitstops").filter(to_date(col("ingestion_date")) == current_date())

# Refine the pitstops DataFrame
df_pitstops_refined = (df_pitstops.drop("_rescued_data")
                       .withColumnRenamed("raceId", "race_id")
                       .withColumnRenamed("driverId", "driver_id")
                       )

df_pitstops_refined = df_pitstops_refined.dropDuplicates().dropna(how="all")
df_pitstops_refined.write.format("delta").mode("append").saveAsTable("capstone_project2.silver.pitstops")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.silver.pitstops

# COMMAND ----------

# Load the Bronze table for lap times
df_lap_times = spark.read.table("capstone_project2.bronze.laptimes").filter(to_date(col("ingestion_date")) == current_date())

# Refine the lap times DataFrame
df_lap_times_refined = (df_lap_times.drop("_rescued_data")
                        .withColumnRenamed("raceId", "race_id")
                        .withColumnRenamed("driverId", "driver_id")
                        )

df_lap_times_refined = df_lap_times_refined.dropDuplicates().dropna(how="all")
df_lap_times_refined.write.format("delta").mode("append").saveAsTable("capstone_project2.silver.lap_times")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.silver.lap_times

# COMMAND ----------

# Load the Bronze table for qualifying
df_qualifying = spark.read.table("capstone_project2.bronze.qualifying").filter(to_date(col("ingestion_date")) == current_date())

# Refine the qualifying DataFrame
df_qualifying_refined = (df_qualifying.drop("_rescued_data")
                         .withColumnRenamed("qualifyId", "qualify_id")
                         .withColumnRenamed("raceId", "race_id")
                         .withColumnRenamed("driverId", "driver_id")
                         .withColumnRenamed("constructorId", "constructor_id")
                         )

df_qualifying_refined = df_qualifying_refined.dropDuplicates().dropna(how="all")
df_qualifying_refined.write.format("delta").mode("append").saveAsTable("capstone_project2.silver.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from capstone_project2.silver.qualifying

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     r.result_id,
# MAGIC     r.race_id,
# MAGIC     r.driver_id,
# MAGIC     r.constructor_id,
# MAGIC     r.position,
# MAGIC     r.points,
# MAGIC     r.laps,
# MAGIC     r.time,
# MAGIC     r.fastest_lap,
# MAGIC     r.fastest_lap_speed,
# MAGIC     d.driver_ref,
# MAGIC     d.name AS driver_name,
# MAGIC     d.nationality AS driver_nationality,
# MAGIC     c.constructor_ref,
# MAGIC     c.name AS constructor_name,
# MAGIC     c.nationality AS constructor_nationality
# MAGIC FROM 
# MAGIC     capstone_project2.silver.results r
# MAGIC JOIN 
# MAGIC     capstone_project2.silver.driver_scd2 d
# MAGIC ON 
# MAGIC     r.driver_id = d.driver_id
# MAGIC JOIN 
# MAGIC     capstone_project2.silver.constructor_scd2 c
# MAGIC ON 
# MAGIC     r.constructor_id = c.constructor_id

# COMMAND ----------

from pyspark.sql.functions import col, avg, count

# Load the Silver tables
df_pitstops = spark.read.table("capstone_project2.silver.pitstops")
df_lap_times = spark.read.table("capstone_project2.silver.lap_times")
df_qualifying = spark.read.table("capstone_project2.silver.qualifying")

# Aggregate pit stops
df_pitstops_agg = df_pitstops.groupBy("race_id", "driver_id").agg(
    count("stop").alias("total_pitstops"),
    avg("duration").alias("avg_pitstop_duration")
)

# Aggregate lap times
df_lap_times_agg = df_lap_times.groupBy("race_id", "driver_id").agg(
    avg("milliseconds").alias("avg_lap_time"),
    count("lap").alias("total_laps")
)

# Select relevant columns from qualifying
df_qualifying_agg = df_qualifying.select(
    "race_id",
    "driver_id",
    "constructor_id",
    "position"
)

# Join the aggregated data
df_consolidated = df_pitstops_agg.join(
    df_lap_times_agg,
    on=["race_id", "driver_id"],
    how="outer"
).join(
    df_qualifying_agg,
    on=["race_id", "driver_id"],
    how="outer"
)

# Write the consolidated DataFrame to a Delta table
df_consolidated.write.format("delta").mode("overwrite").saveAsTable("capstone_project2.silver.consolidated_race_data")

# Display the consolidated DataFrame
display(df_consolidated)

# COMMAND ----------

from pyspark.sql.functions import col, when, expr

# Load the results table
df_results = spark.read.table("capstone_project2.silver.results")

# Normalize the time column
df_normalized_results = df_results.withColumn(
    "normalized_time",
    when(col("position") == 1, col("time"))
    .otherwise(expr("substring(time, 2, length(time))"))
)

# Create a temporary view
df_normalized_results.createOrReplaceTempView("normalized_results")
