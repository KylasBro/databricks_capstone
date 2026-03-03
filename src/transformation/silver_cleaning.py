from pyspark.sql.functions import col

def clean_data(df):
    return (
        df.dropna()
          .withColumn("year", col("year").cast("int"))
          .withColumn("enrollment", col("enrollment").cast("int"))
    )