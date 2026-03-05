# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer - Raw Data Ingestion
# MAGIC This notebook ingests raw data into the Bronze layer of Unity Catalog.

# COMMAND ----------

# Unity Catalog Configuration
CATALOG = "capstone_kailas"
SCHEMA = "school_data"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_enrollment"

# COMMAND ----------

# Note: Catalog and schema must be created manually in Databricks UI
# CREATE CATALOG education_catalog
# CREATE SCHEMA education_catalog.school_data

# Set the current catalog and schema
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

dbutils.fs.cp(
    "file:/Workspace/Users/kailas@purpletalk.onmicrosoft.com/databricks_capstone/data/raw/school_enrollment.csv",
    "dbfs:/FileStore/school_enrollment.csv"
)

# COMMAND ----------

# Read raw CSV data
input_path = "dbfs:/FileStore/school_enrollment.csv"
df_bronze = spark.read.csv(input_path, header=True, inferSchema=True)

# COMMAND ----------

# Write to Unity Catalog Bronze table
df_bronze.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(BRONZE_TABLE)

print(f"Bronze data written to: {BRONZE_TABLE}")
display(df_bronze)

# COMMAND ----------

print("Bronze layer ingestion completed successfully.")