"""
Gold Layer Aggregation Module

This module handles data aggregation for the Gold layer of the medallion
architecture. It creates business-level aggregations and summary tables
ready for analytics and reporting.
"""

from pyspark.sql.functions import avg, count, min, max, stddev, round, sum, col, when


def aggregate_yearly(df):
    """
    Aggregate score data by academic year for the Gold layer.
    
    This function creates a yearly summary with comprehensive metrics,
    suitable for trend analysis and reporting dashboards.
    
    Args:
        df (DataFrame): Cleaned DataFrame from the Silver layer.
    
    Returns:
        DataFrame: Aggregated DataFrame with yearly metrics.
            Columns:
            - academic_year (int): The academic year
            - avg_score (double): Average score for the year
            - min_score (double): Minimum score for the year
            - max_score (double): Maximum score for the year
            - stddev_score (double): Standard deviation of scores
            - total_students (long): Total student count
            - high_performers (long): Count of high performers
            - low_performers (long): Count of low performers
    
    Example:
        >>> yearly_summary = aggregate_yearly(silver_df)
    """
    return (
        df.groupBy("academic_year")
        .agg(
            round(avg("avg_score"), 2).alias("avg_score"),
            round(min("avg_score"), 2).alias("min_score"),
            round(max("avg_score"), 2).alias("max_score"),
            round(stddev("avg_score"), 2).alias("stddev_score"),
            count("*").alias("total_students"),
            sum(when(col("score_category") == "High", 1).otherwise(0)).alias("high_performers"),
            sum(when(col("score_category") == "Low", 1).otherwise(0)).alias("low_performers")
        )
        .orderBy("academic_year")
    )


def aggregate_by_region(df):
    """
    Aggregate score data by region and academic year.
    
    Args:
        df (DataFrame): Cleaned DataFrame from the Silver layer.
    
    Returns:
        DataFrame: Aggregated DataFrame with region-level metrics.
    """
    return (
        df.groupBy("region", "academic_year")
        .agg(
            round(avg("avg_score"), 2).alias("avg_score"),
            round(min("avg_score"), 2).alias("min_score"),
            round(max("avg_score"), 2).alias("max_score"),
            count("*").alias("total_students"),
            sum(when(col("score_category") == "High", 1).otherwise(0)).alias("high_performers")
        )
        .orderBy("region", "academic_year")
    )


def aggregate_by_gender(df):
    """
    Aggregate score data by gender and academic year.
    
    Args:
        df (DataFrame): Cleaned DataFrame from the Silver layer.
    
    Returns:
        DataFrame: Aggregated DataFrame with gender-level metrics.
    """
    return (
        df.groupBy("gender", "academic_year")
        .agg(
            round(avg("avg_score"), 2).alias("avg_score"),
            round(min("avg_score"), 2).alias("min_score"),
            round(max("avg_score"), 2).alias("max_score"),
            count("*").alias("total_students"),
            sum(when(col("score_category") == "High", 1).otherwise(0)).alias("high_performers")
        )
        .orderBy("gender", "academic_year")
    )