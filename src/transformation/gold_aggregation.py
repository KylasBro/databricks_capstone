from pyspark.sql.functions import sum

def aggregate_yearly(df):
    return (
        df.groupBy("year")
          .agg(sum("enrollment").alias("total_enrollment"))
    )