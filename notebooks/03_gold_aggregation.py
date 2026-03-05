# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer - Aggregated Analytics
# MAGIC This notebook creates business-level aggregations in Unity Catalog.

# COMMAND ----------

from pyspark.sql.functions import avg, count

# Unity Catalog Configuration
CATALOG = "education_catalog"
SCHEMA = "school_data"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_enrollment"
GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_yearly_summary"

# COMMAND ----------

# Read from Silver table in Unity Catalog
df_silver = spark.table(SILVER_TABLE)
print(f"Reading from: {SILVER_TABLE}")
display(df_silver)

# COMMAND ----------

# Aggregate by academic year
df_gold = (
    df_silver.groupBy("academic_year")
    .agg(
        avg("avg_score").alias("avg_score_total"),
        count("*").alias("record_count")
    )
    .orderBy("academic_year")
)

# COMMAND ----------

# Write to Unity Catalog Gold table
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_TABLE)

print(f"Gold data written to: {GOLD_TABLE}")
display(df_gold)

# COMMAND ----------

print("Gold layer aggregation completed.")