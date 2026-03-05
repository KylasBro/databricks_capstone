"""
Silver Layer Data Cleaning Module

This module handles data transformation and cleaning for the Silver layer
of the medallion architecture. It applies data quality rules, removes
invalid records, and standardizes data types.
"""

from pyspark.sql.functions import col


def clean_data(df):
    """
    Clean and transform data for the Silver layer.
    
    This function applies data quality transformations including:
    - Removing rows with null values
    - Casting columns to appropriate data types
    
    Args:
        df (DataFrame): Input DataFrame from the Bronze layer.
    
    Returns:
        DataFrame: Cleaned DataFrame with proper data types and no null values.
    
    Example:
        >>> cleaned_df = clean_data(bronze_df)
    """
    return (
        df.dropna()  # Remove rows with any null values
          .withColumn("academic_year", col("academic_year").cast("int"))  # Cast academic_year to integer type
          .withColumn("avg_score", col("avg_score").cast("double"))  # Cast avg_score to double type
    )