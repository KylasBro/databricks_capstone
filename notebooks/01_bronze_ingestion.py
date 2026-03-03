# Databricks notebook source
# MAGIC %md
# MAGIC ## Bronze Layer - Raw Data Ingestion

# COMMAND ----------

from src.utils.spark_session import get_spark_session
from src.ingestion.bronze_loader import load_raw_data

spark = get_spark_session("BronzeIngestion")

# COMMAND ----------

input_path = "dbfs:/FileStore/school_enrollment.csv"
output_path = "dbfs:/mnt/bronze/school_enrollment"

df_bronze = load_raw_data(spark, input_path, output_path)

display(df_bronze)

# COMMAND ----------

print("Bronze layer ingestion completed successfully.")