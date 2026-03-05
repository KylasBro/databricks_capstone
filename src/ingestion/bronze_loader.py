"""
Bronze Layer Data Loader Module

This module handles the ingestion of raw data into the Bronze layer
of the medallion architecture. It reads CSV files and stores them
as Delta tables for downstream processing.
"""


def load_raw_data(spark, input_path, output_path):
    """
    Load raw CSV data into the Bronze layer as a Delta table.
    
    This function performs the initial data ingestion step in the ETL pipeline.
    It reads raw CSV data with headers and inferred schema, then persists it
    as a Delta table in the Bronze layer.
    
    Args:
        spark (SparkSession): Active Spark session for data processing.
        input_path (str): Path to the source CSV file(s) to ingest.
        output_path (str): Destination path for the Bronze Delta table.
    
    Returns:
        DataFrame: The ingested DataFrame containing raw data.
    
    Example:
        >>> spark = get_spark_session()
        >>> df = load_raw_data(spark, "data/raw/enrollment.csv", "dbfs:/bronze/enrollment")
    """
    # Read raw CSV data with header row and automatic schema inference
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    # Write to Delta format in Bronze layer, overwriting existing data
    df.write.format("delta") \
        .mode("overwrite") \
        .save(output_path)
    
    return df