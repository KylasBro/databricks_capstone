# Databricks notebook source
# MAGIC %md
# MAGIC ## Data Quality Checks

# COMMAND ----------

from networkx import display

from src.utils.spark_session import get_spark_session
from src.validation.data_quality import check_nulls

spark = get_spark_session("DataQuality")

# COMMAND ----------

silver_path = "dbfs:/mnt/silver/school_enrollment"

df_silver = spark.read.format("delta").load(silver_path)

# COMMAND ----------

is_valid = check_nulls(df_silver)

if is_valid:
    print("Data quality check passed ✅")
else:
    raise Exception("Data quality check failed ❌")

# COMMAND ----------

display(df_silver.describe())