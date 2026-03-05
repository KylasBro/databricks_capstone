# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Quality Checks
# MAGIC This notebook validates data quality in Unity Catalog tables.

# COMMAND ----------

# Unity Catalog Configuration
CATALOG = "capstone_kailas"
SCHEMA = "school_data"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_enrollment"

# COMMAND ----------

# Read from Silver table in Unity Catalog
df_silver = spark.table(SILVER_TABLE)
print(f"Validating: {SILVER_TABLE}")

# COMMAND ----------

# Check for null values in critical columns
null_count = df_silver.filter("academic_year IS NULL OR avg_score IS NULL").count()

if null_count == 0:
    print("✅ Data quality check passed: No null values in critical columns")
else:
    raise Exception(f"❌ Data quality check failed: {null_count} rows with null values")

# COMMAND ----------

# Display summary statistics
print("\nData Summary:")
print(f"Total records: {df_silver.count()}")
display(df_silver.describe())

# COMMAND ----------

# Validate data types
print("\nSchema:")
df_silver.printSchema()

# COMMAND ----------

print("Data quality checks completed successfully.")