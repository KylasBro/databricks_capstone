# Databricks notebook source
# MAGIC %md
# MAGIC ## Gold Layer - Aggregated Analytics

# COMMAND ----------

from src.utils.spark_session import get_spark_session
from src.transformation.gold_aggregation import aggregate_yearly

spark = get_spark_session("GoldAggregation")

# COMMAND ----------

silver_path = "dbfs:/mnt/silver/school_enrollment"
gold_path = "dbfs:/mnt/gold/school_enrollment"

df_silver = spark.read.format("delta").load(silver_path)

# COMMAND ----------

df_gold = aggregate_yearly(df_silver)

df_gold.write.format("delta") \
    .mode("overwrite") \
    .save(gold_path)

display(df_gold)

# COMMAND ----------

print("Gold layer aggregation completed.")