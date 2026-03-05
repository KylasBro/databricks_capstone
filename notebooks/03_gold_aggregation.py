# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer - Aggregated Analytics
# MAGIC This notebook creates business-level aggregations in Unity Catalog.

# COMMAND ----------

from pyspark.sql.functions import avg, count, min, max, stddev, round, sum, col, when

# Unity Catalog Configuration
CATALOG = "capstone_kailas"
SCHEMA = "school_data"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_enrollment"
GOLD_YEARLY_TABLE = f"{CATALOG}.{SCHEMA}.gold_yearly_summary"
GOLD_REGION_TABLE = f"{CATALOG}.{SCHEMA}.gold_region_summary"
GOLD_GENDER_TABLE = f"{CATALOG}.{SCHEMA}.gold_gender_summary"

# COMMAND ----------

# Read from Silver table in Unity Catalog
df_silver = spark.table(SILVER_TABLE)
print(f"Reading from: {SILVER_TABLE}")
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 1: Yearly Summary

# COMMAND ----------

# Aggregate by academic year with comprehensive metrics
df_gold_yearly = (
    df_silver.groupBy("academic_year")
    .agg(
        round(avg("avg_score"), 2).alias("avg_score"),
        round(min("avg_score"), 2).alias("min_score"),
        round(max("avg_score"), 2).alias("max_score"),
        round(stddev("avg_score"), 2).alias("stddev_score"),
        count("*").alias("total_students"),
        sum(when(col("score_category") == "High", 1).otherwise(0)).alias("high_performers"),
        sum(when(col("score_category") == "Low", 1).otherwise(0)).alias("low_performers")
    )
    .orderBy("academic_year")
)

# Write yearly summary
df_gold_yearly.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_YEARLY_TABLE)

print(f"Gold yearly data written to: {GOLD_YEARLY_TABLE}")
display(df_gold_yearly)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 2: Region Summary

# COMMAND ----------

# Aggregate by region
df_gold_region = (
    df_silver.groupBy("region", "academic_year")
    .agg(
        round(avg("avg_score"), 2).alias("avg_score"),
        round(min("avg_score"), 2).alias("min_score"),
        round(max("avg_score"), 2).alias("max_score"),
        count("*").alias("total_students"),
        sum(when(col("score_category") == "High", 1).otherwise(0)).alias("high_performers")
    )
    .orderBy("region", "academic_year")
)

# Write region summary
df_gold_region.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_REGION_TABLE)

print(f"Gold region data written to: {GOLD_REGION_TABLE}")
display(df_gold_region)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 3: Gender Summary

# COMMAND ----------

# Aggregate by gender
df_gold_gender = (
    df_silver.groupBy("gender", "academic_year")
    .agg(
        round(avg("avg_score"), 2).alias("avg_score"),
        round(min("avg_score"), 2).alias("min_score"),
        round(max("avg_score"), 2).alias("max_score"),
        count("*").alias("total_students"),
        sum(when(col("score_category") == "High", 1).otherwise(0)).alias("high_performers")
    )
    .orderBy("gender", "academic_year")
)

# Write gender summary
df_gold_gender.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(GOLD_GENDER_TABLE)

print(f"Gold gender data written to: {GOLD_GENDER_TABLE}")
display(df_gold_gender)

# COMMAND ----------

print("Gold layer aggregation completed - 3 tables created.")