"""
Gold Layer Aggregation Module

This module handles data aggregation for the Gold layer of the medallion
architecture. It creates business-level aggregations and summary tables
ready for analytics and reporting.
"""

from pyspark.sql.functions import sum, avg


def aggregate_yearly(df):
    """
    Aggregate score data by academic year for the Gold layer.
    
    This function creates a yearly summary of average scores,
    suitable for trend analysis and reporting dashboards.
    
    Args:
        df (DataFrame): Cleaned DataFrame from the Silver layer.
    
    Returns:
        DataFrame: Aggregated DataFrame with yearly score averages.
            Columns:
            - academic_year (int): The academic year
            - avg_score_total (double): Average score for the year
    
    Example:
        >>> yearly_summary = aggregate_yearly(silver_df)
    """
    return (
        df.groupBy("academic_year")  # Group records by academic year
          .agg(avg("avg_score").alias("avg_score_total"))  # Calculate average score per year
    )