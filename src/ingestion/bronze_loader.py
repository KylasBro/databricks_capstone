def load_raw_data(spark, input_path, output_path):
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    df.write.format("delta") \
        .mode("overwrite") \
        .save(output_path)
    
    return df