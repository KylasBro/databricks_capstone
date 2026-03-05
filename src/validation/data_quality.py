"""
Data Quality Validation Module

This module provides data quality checks and validation functions
for the ETL pipeline. It ensures data integrity at various stages
of the medallion architecture.
"""


def check_nulls(df):
    """
    Validate that critical columns have no null values.
    
    Args:
        df (DataFrame): DataFrame to validate.
    
    Returns:
        bool: True if no null values exist in critical columns.
    """
    return df.filter("academic_year IS NULL OR avg_score IS NULL").count() == 0


def check_score_range(df):
    """
    Validate that all scores are within the valid range (0-100).
    
    Args:
        df (DataFrame): DataFrame to validate.
    
    Returns:
        bool: True if all scores are within valid range.
    """
    from pyspark.sql.functions import col
    return df.filter((col("avg_score") < 0) | (col("avg_score") > 100)).count() == 0


def check_score_categories(df):
    """
    Validate that all score categories are valid values.
    
    Args:
        df (DataFrame): DataFrame to validate.
    
    Returns:
        bool: True if all categories are valid (High, Medium, Low).
    """
    from pyspark.sql.functions import col
    valid_categories = ["High", "Medium", "Low"]
    return df.filter(~col("score_category").isin(valid_categories)).count() == 0


def run_all_checks(df):
    """
    Run all data quality checks on a DataFrame.
    
    Args:
        df (DataFrame): DataFrame to validate.
    
    Returns:
        dict: Dictionary with check names and their results.
    """
    return {
        "null_check": check_nulls(df),
        "score_range_check": check_score_range(df),
        "category_check": check_score_categories(df)
    }