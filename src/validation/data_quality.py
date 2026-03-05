"""
Data Quality Validation Module

This module provides data quality checks and validation functions
for the ETL pipeline. It ensures data integrity at various stages
of the medallion architecture.
"""


def check_nulls(df):
    """
    Validate that critical columns have no null values.
    
    This function checks if the 'academic_year' and 'avg_score' columns
    contain any null values, which would indicate data quality issues.
    
    Args:
        df (DataFrame): DataFrame to validate.
    
    Returns:
        bool: True if no null values exist in critical columns,
              False otherwise.
    
    Example:
        >>> is_valid = check_nulls(silver_df)
        >>> if not is_valid:
        ...     raise Exception("Data quality check failed: null values found")
    """
    # Filter for rows with null values in critical columns and check if count is zero
    return df.filter("academic_year IS NULL OR avg_score IS NULL").count() == 0