# Databricks notebook source
# MAGIC %md
# MAGIC ## Silver Layer - Data Cleaning & Transformation

# COMMAND ----------

from src.utils.spark_session import get_spark_session
from src.transformation.silver_cleaning import clean_data

spark = get_spark_session("SilverTransformation")

# COMMAND ----------

bronze_path = "dbfs:/mnt/bronze/school_enrollment"
silver_path = "dbfs:/mnt/silver/school_enrollment"

df_bronze = spark.read.format("delta").load(bronze_path)

# COMMAND ----------

df_silver = clean_data(df_bronze)

df_silver.write.format("delta") \
    .mode("overwrite") \
    .save(silver_path)

display(df_silver)

# COMMAND ----------

print("Silver layer transformation completed.")