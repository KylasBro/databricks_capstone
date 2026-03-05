# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer - Raw Data Ingestion
# MAGIC This notebook ingests raw data into the Bronze layer of Unity Catalog.

# COMMAND ----------

# Unity Catalog Configuration
CATALOG = "education_catalog"
SCHEMA = "school_data"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_enrollment"

# COMMAND ----------

# Create catalog and schema if they don't exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

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