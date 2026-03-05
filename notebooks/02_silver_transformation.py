# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer - Data Cleaning & Transformation
# MAGIC This notebook transforms Bronze data and writes to Silver layer in Unity Catalog.

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, lower, when, regexp_replace

# Unity Catalog Configuration
CATALOG = "capstone_kailas"
SCHEMA = "school_data"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_enrollment"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_enrollment"

# COMMAND ----------

# Read from Bronze table in Unity Catalog
df_bronze = spark.table(BRONZE_TABLE)
print(f"Reading from: {BRONZE_TABLE}")
print(f"Bronze record count: {df_bronze.count()}")
display(df_bronze)

# COMMAND ----------

# Clean and transform data
df_silver = (
    df_bronze
    # Remove rows with null values in critical columns
    .dropna(subset=["academic_year", "avg_score"])
    
    # Cast columns to appropriate data types
    .withColumn("academic_year", col("academic_year").cast("int"))
    .withColumn("avg_score", col("avg_score").cast("double"))
    
    # Trim whitespace from string columns
    .withColumn("gender", trim(col("gender")))
    .withColumn("grade", trim(col("grade")))
    .withColumn("region", trim(col("region")))
    
    # Standardize gender values to uppercase
    .withColumn("gender", upper(col("gender")))
    
    # Standardize region values to title case
    .withColumn("region", regexp_replace(col("region"), "_", " "))
    
    # Filter out invalid scores (must be between 0 and 100)
    .filter((col("avg_score") >= 0) & (col("avg_score") <= 100))
    
    # Add score category based on performance
    .withColumn("score_category", 
        when(col("avg_score") >= 80, "High")
        .when(col("avg_score") >= 50, "Medium")
        .otherwise("Low")
    )
)

print(f"Silver record count after cleaning: {df_silver.count()}")

# COMMAND ----------

# Write to Unity Catalog Silver table
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(SILVER_TABLE)

print(f"Silver data written to: {SILVER_TABLE}")
display(df_silver)

# COMMAND ----------

print("Silver layer transformation completed.")