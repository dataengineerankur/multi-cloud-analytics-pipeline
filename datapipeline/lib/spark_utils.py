"""
Spark Utility Functions for Psycho Bunny Data Pipeline
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

logger = logging.getLogger(__name__)

def create_spark_session_with_deequ(app_name, config=None):
    """
    Create Spark session with Delta Lake and Deequ support
    
    Args:
        app_name (str): Name of the Spark application
        config (dict): Additional Spark configuration
    
    Returns:
        SparkSession: Configured Spark session with Deequ
    """
    # Set SPARK_VERSION environment variable required by pydeequ
    os.environ["SPARK_VERSION"] = "3.4"
    
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "com.amazon.deequ:deequ:2.0.4-spark-3.4")
    
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    logger.info(f"Spark session with Deequ created: {app_name}")
    return spark

def create_spark_session(app_name, config=None):
    """
    Create Spark session with Delta Lake support
    
    Args:
        app_name (str): Name of the Spark application
        config (dict): Additional Spark configuration
    
    Returns:
        SparkSession: Configured Spark session
    """
    builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    if config:
        for key, value in config.items():
            builder = builder.config(key, value)
    
    spark = builder.getOrCreate()
    logger.info(f"Spark session created: {app_name}")
    return spark

def read_csv_with_schema(spark, file_path, schema=None, header=True):
    """
    Read CSV file with optional schema
    
    Args:
        spark: SparkSession
        file_path (str): Path to CSV file
        schema: Optional schema
        header (bool): Whether file has header
    
    Returns:
        DataFrame: Loaded DataFrame
    """
    reader = spark.read.option("header", str(header).lower())
    
    if schema:
        reader = reader.schema(schema)
    else:
        reader = reader.option("inferSchema", "true")
    
    return reader.csv(file_path)

def write_delta_table(df, path, mode="overwrite", partition_by=None):
    """
    Write DataFrame to Delta Lake
    
    Args:
        df: DataFrame to write
        path (str): Target path
        mode (str): Write mode
        partition_by (list): Columns to partition by
    """
    writer = df.write.format("delta").mode(mode)
    
    if partition_by:
        writer = writer.partitionBy(partition_by)
    
    writer.save(path)
    logger.info(f"Data written to Delta table: {path}")

def add_audit_columns(df):
    """
    Add standard audit columns to DataFrame
    
    Args:
        df: Source DataFrame
    
    Returns:
        DataFrame: DataFrame with audit columns
    """
    return df.withColumn("created_date", current_timestamp()) \
             .withColumn("updated_date", current_timestamp())

def deduplicate_data(df, key_columns):
    """
    Remove duplicates based on key columns
    
    Args:
        df: Source DataFrame
        key_columns (list): Columns to use for deduplication
    
    Returns:
        DataFrame: Deduplicated DataFrame
    """
    original_count = df.count()
    deduped_df = df.dropDuplicates(key_columns)
    final_count = deduped_df.count()
    
    logger.info(f"Deduplication: {original_count} -> {final_count} records")
    return deduped_df

def validate_data_quality(df, rules):
    """
    Validate data quality based on rules
    
    Args:
        df: DataFrame to validate
        rules (dict): Validation rules
    
    Returns:
        dict: Validation results
    """
    results = {}
    
    for column, rule in rules.items():
        if column in df.columns:
            if rule.get("null_check"):
                null_count = df.filter(col(column).isNull()).count()
                results[f"{column}_null_count"] = null_count
            
            if rule.get("unique_check"):
                total_count = df.count()
                distinct_count = df.select(column).distinct().count()
                results[f"{column}_unique_ratio"] = distinct_count / total_count if total_count > 0 else 0
    
    return results

def create_customer_id(first_name, last_name, email):
    """
    Create unique customer ID from name and email
    
    Args:
        first_name (str): Customer first name
        last_name (str): Customer last name
        email (str): Customer email
    
    Returns:
        str: Unique customer ID
    """
    return f"{first_name}_{last_name}_{email}".replace(" ", "_").lower()

def extract_product_family(product_code):
    """
    Extract product family from product code
    
    Args:
        product_code (str): Product code (e.g., S10_4757)
    
    Returns:
        str: Product family (e.g., S10)
    """
    if product_code and "_" in product_code:
        return product_code.split("_")[0]
    return "UNKNOWN"

def calculate_restocking_fee(amount, status, fee_rate=0.10):
    """
    Calculate restocking fee for refunds
    
    Args:
        amount (float): Transaction amount
        status (str): Transaction status
        fee_rate (float): Fee rate (default 10%)
    
    Returns:
        float: Restocking fee amount
    """
    if status and "refund" in status.lower():
        return float(amount) * fee_rate
    return 0.0

# UDF definitions
create_customer_id_udf = udf(create_customer_id, StringType())
extract_product_family_udf = udf(extract_product_family, StringType())
calculate_restocking_fee_udf = udf(calculate_restocking_fee, DoubleType()) 