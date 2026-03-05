"""
Spark Session Utility Module

This module provides utility functions for managing Spark sessions
across the ETL pipeline. It ensures consistent Spark configuration
and session management.
"""

from pyspark.sql import SparkSession


def get_spark_session(app_name="SchoolEnrollmentApp"):
    """
    Get or create a Spark session for the ETL pipeline.
    
    This function creates a new Spark session or retrieves an existing one.
    It provides a centralized way to manage Spark sessions across all
    pipeline components.
    
    Args:
        app_name (str, optional): Name for the Spark application.
            Defaults to "SchoolEnrollmentApp".
    
    Returns:
        SparkSession: Active Spark session configured for the application.
    
    Example:
        >>> spark = get_spark_session()
        >>> spark = get_spark_session(app_name="CustomPipeline")
    """
    return (
        SparkSession.builder
        .appName(app_name)  # Set the application name for Spark UI
        .getOrCreate()  # Get existing session or create new one
    )