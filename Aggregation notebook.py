# Databricks notebook source
from pyspark.sql.functions import col, sum, avg, count, when

# Load the Silver tables
df_results = spark.read.table("capstone_project2.silver.results")
df_races = spark.read.table("capstone_project2.silver.races")

# Join results with races to get the season information
df_results_with_season = df_results.join(
    df_races,
    df_results.race_id == df_races.race_id,
    "inner"
).select(
    df_results.driver_id,
    df_races.race_year.alias("season"),
    df_results.points,
    df_results.position
)

# Aggregate data for driver performance
df_driver_performance = df_results_with_season.groupBy("driver_id", "season").agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins"),
    count(when(col("position") <= 3, True)).alias("podiums"),
    avg("position").alias("avg_position")
)
df_driver_performance.write.mode("overwrite").saveAsTable("capstone_project2.gold.driver_performance")

# Display the aggregated DataFrame
display(df_driver_performance)

# COMMAND ----------

from pyspark.sql.functions import col, sum, count, when

# Load the Silver tables
df_results = spark.read.table("capstone_project2.silver.results")
df_races = spark.read.table("capstone_project2.silver.races")

# Join results with races to get the season information
df_results_with_season = df_results.join(
    df_races,
    df_results.race_id == df_races.race_id,
    "inner"
).select(
    df_results.constructor_id,
    df_races.race_year.alias("season"),
    df_results.points,
    df_results.position
)

# Aggregate data for constructor standings
df_constructor_standings = df_results_with_season.groupBy("constructor_id", "season").agg(
    sum("points").alias("total_points"),
    count(when(col("position") == 1, True)).alias("wins")
)

df_constructor_standings.write.mode("overwrite").saveAsTable("capstone_project2.gold.constructor_standings")

# Display the aggregated DataFrame
display(df_constructor_standings)

# COMMAND ----------

from pyspark.sql.functions import col, avg, count

# Load the Silver tables
df_pitstops = spark.read.table("capstone_project2.silver.pitstops")
df_lap_times = spark.read.table("capstone_project2.silver.lap_times")
df_results = spark.read.table("capstone_project2.silver.results")
df_races = spark.read.table("capstone_project2.silver.races")

# Join pitstops with races to get the circuit information
df_pitstops_with_circuit = df_pitstops.join(
    df_races,
    df_pitstops.race_id == df_races.race_id,
    "inner"
).select(
    df_races.circuit_id,
    df_pitstops.race_id,
    df_pitstops.driver_id,
    df_pitstops.stop
)

# Aggregate pit stop frequencies per circuit
df_pitstops_agg = df_pitstops_with_circuit.groupBy("circuit_id").agg(
    count("stop").alias("total_pitstops")
)

# Join lap times with races to get the circuit information
df_lap_times_with_circuit = df_lap_times.join(
    df_races,
    df_lap_times.race_id == df_races.race_id,
    "inner"
).select(
    df_races.circuit_id,
    df_lap_times.race_id,
    df_lap_times.driver_id,
    df_lap_times.milliseconds
)

# Aggregate average lap times per circuit
df_lap_times_agg = df_lap_times_with_circuit.groupBy("circuit_id").agg(
    avg("milliseconds").alias("avg_lap_time")
)

# Join results with races to get the circuit information
df_results_with_circuit = df_results.join(
    df_races,
    df_results.race_id == df_races.race_id,
    "inner"
).select(
    df_races.circuit_id,
    df_results.race_id,
    df_results.driver_id,
    df_results.position
)

# Aggregate finishing positions per circuit
df_results_agg = df_results_with_circuit.groupBy("circuit_id").agg(
    avg("position").alias("avg_position")
)

# Join the aggregated data
df_circuit_insights = df_pitstops_agg.join(
    df_lap_times_agg,
    on="circuit_id",
    how="outer"
).join(
    df_results_agg,
    on="circuit_id",
    how="outer"
)

df_circuit_insights.write.mode("overwrite").saveAsTable("capstone_project2.gold.circuit_insights")

# Display the aggregated DataFrame
display(df_circuit_insights)

# COMMAND ----------

from delta.tables import DeltaTable

# Optimize and Z-order the driver performance table
driver_performance_table = DeltaTable.forName(spark, "capstone_project2.gold.driver_performance")
driver_performance_table.optimize().executeCompaction()


# Optimize and Z-order the constructor standings table
constructor_standings_table = DeltaTable.forName(spark, "capstone_project2.gold.constructor_standings")
constructor_standings_table.optimize().executeCompaction()


# Optimize and Z-order the circuit insights table
circuit_insights_table = DeltaTable.forName(spark, "capstone_project2.gold.circuit_insights")
circuit_insights_table.optimize().executeCompaction()

# COMMAND ----------

# Execute SQL command to optimize and Z-Order the driver_performance table
spark.sql("""
OPTIMIZE capstone_project2.gold.driver_performance
ZORDER BY (driver_id)
""")

# Execute SQL command to optimize and Z-Order the constructor_standings table
spark.sql("""
OPTIMIZE capstone_project2.gold.constructor_standings
ZORDER BY (constructor_id)
""")

# Execute SQL command to optimize and Z-Order the circuit_insights table
spark.sql("""
OPTIMIZE capstone_project2.gold.circuit_insights
ZORDER BY (circuit_id)
""")
