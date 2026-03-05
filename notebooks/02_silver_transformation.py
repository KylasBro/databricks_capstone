# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer - Data Cleaning & Transformation
# MAGIC This notebook transforms Bronze data and writes to Silver layer in Unity Catalog.

# COMMAND ----------

from pyspark.sql.functions import col

# Unity Catalog Configuration
CATALOG = "education_catalog"
SCHEMA = "school_data"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_enrollment"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_enrollment"

# COMMAND ----------

# Read from Bronze table in Unity Catalog
df_bronze = spark.table(BRONZE_TABLE)
print(f"Reading from: {BRONZE_TABLE}")
display(df_bronze)

# COMMAND ----------

# Clean and transform data
df_silver = (
    df_bronze.dropna()  # Remove rows with null values
    .withColumn("academic_year", col("academic_year").cast("int"))
    .withColumn("avg_score", col("avg_score").cast("double"))
)

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