"""
Silver Layer Data Cleaning Module

This module handles data transformation and cleaning for the Silver layer
of the medallion architecture. It applies data quality rules, removes
invalid records, and standardizes data types.
"""

from pyspark.sql.functions import col, trim, upper, when, regexp_replace


def clean_data(df):
    """
    Clean and transform data for the Silver layer.
    
    This function applies data quality transformations including:
    - Removing rows with null values in critical columns
    - Casting columns to appropriate data types
    - Trimming whitespace from string columns
    - Standardizing gender to uppercase
    - Filtering invalid scores (outside 0-100 range)
    - Adding score_category derived column
    
    Args:
        df (DataFrame): Input DataFrame from the Bronze layer.
    
    Returns:
        DataFrame: Cleaned DataFrame with proper data types and no null values.
    
    Example:
        >>> cleaned_df = clean_data(bronze_df)
    """
    return (
        df
        # Remove rows with null values in critical columns
        .dropna(subset=["academic_year", "avg_score"])
        
        # Cast columns to appropriate data types
        .withColumn("academic_year", col("academic_year").cast("int"))
        .withColumn("avg_score", col("avg_score").cast("double"))
        
        # Trim whitespace from string columns
        .withColumn("gender", trim(col("gender")))
        .withColumn("grade", trim(col("grade")))
        .withColumn("region", trim(col("region")))
        
        # Standardize gender values to uppercase
        .withColumn("gender", upper(col("gender")))
        
        # Standardize region values (replace underscores with spaces)
        .withColumn("region", regexp_replace(col("region"), "_", " "))
        
        # Filter out invalid scores (must be between 0 and 100)
        .filter((col("avg_score") >= 0) & (col("avg_score") <= 100))
        
        # Add score category based on performance
        .withColumn("score_category", 
            when(col("avg_score") >= 80, "High")
            .when(col("avg_score") >= 50, "Medium")
            .otherwise("Low")
        )
    )