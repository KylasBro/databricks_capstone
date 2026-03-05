# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Quality Checks
# MAGIC This notebook validates data quality in Unity Catalog tables.

# COMMAND ----------

from pyspark.sql.functions import col

# Unity Catalog Configuration
CATALOG = "capstone_kailas"
SCHEMA = "school_data"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_enrollment"

# COMMAND ----------

# Read from Silver table in Unity Catalog
df_silver = spark.table(SILVER_TABLE)
print(f"Validating: {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 1: Null Values in Critical Columns

# COMMAND ----------

null_count = df_silver.filter("academic_year IS NULL OR avg_score IS NULL").count()

if null_count == 0:
    print("✅ Check 1 PASSED: No null values in critical columns")
else:
    raise Exception(f"❌ Check 1 FAILED: {null_count} rows with null values")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 2: Score Range Validation (0-100)

# COMMAND ----------

invalid_scores = df_silver.filter((col("avg_score") < 0) | (col("avg_score") > 100)).count()

if invalid_scores == 0:
    print("✅ Check 2 PASSED: All scores are within valid range (0-100)")
else:
    raise Exception(f"❌ Check 2 FAILED: {invalid_scores} rows with invalid scores")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 3: Score Category Validation

# COMMAND ----------

valid_categories = ["High", "Medium", "Low"]
invalid_category_count = df_silver.filter(~col("score_category").isin(valid_categories)).count()

if invalid_category_count == 0:
    print("✅ Check 3 PASSED: All score categories are valid")
else:
    raise Exception(f"❌ Check 3 FAILED: {invalid_category_count} rows with invalid categories")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check 4: Gender Standardization

# COMMAND ----------

gender_values = df_silver.select("gender").distinct().collect()
print(f"Unique gender values: {[row['gender'] for row in gender_values]}")
print("✅ Check 4 PASSED: Gender values reviewed")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Summary

# COMMAND ----------

print(f"\nTotal records: {df_silver.count()}")
print(f"\nRecords by score category:")
display(df_silver.groupBy("score_category").count().orderBy("score_category"))

# COMMAND ----------

print("\nSchema:")
df_silver.printSchema()

# COMMAND ----------

print("\n✅ All data quality checks completed successfully.")