#!/usr/bin/env python3
"""
Data Ingestion Script
Loads raw data from S3 landing zone and stores in Delta Lake format
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv

# Load environment variables
load_dotenv('datapipeline/config.env')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Delta Lake and S3 support"""
    return SparkSession.builder \
        .appName("PsychoBunny-DataIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def load_customers(spark, s3_bucket):
    """Load customer data from multiple CSV files"""
    customers_raw = spark.read \
        .option("header", "true") \
        .option("mergeSchema", "true") \
        .csv(f"s3://{s3_bucket}/landing-zone/customers/*.csv") \
        .withColumn("source_file", input_file_name()) \
        .withColumn("ingestion_date", current_timestamp()) \
        .withColumn("customer_id", concat_ws("_", lower(col("first_name")), lower(col("last_name")), lower(col("email")))) \
        .dropDuplicates(["customer_id"])
    
    logger.info(f"Loaded {customers_raw.count()} unique customers")
    return customers_raw

def load_transactions(spark, s3_bucket):
    """Load transaction data"""
    transactions_path = f"s3://{s3_bucket}/landing-zone/transactions/de_shop_transactions_20230821.csv"
    
    transactions_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(transactions_path) \
        .withColumn("source_file", lit("de_shop_transactions_20230821.csv")) \
        .withColumn("ingestion_date", current_timestamp())
    
    logger.info(f"Loaded transaction data: {transactions_raw.count()} records")
    return transactions_raw

def load_calendar(spark, s3_bucket):
    """Load fiscal calendar data"""
    calendar_path = f"s3://{s3_bucket}/landing-zone/calendar/de_dates.csv"
    
    calendar_raw = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(calendar_path) \
        .withColumn("source_file", lit("de_dates.csv")) \
        .withColumn("ingestion_date", current_timestamp())
    
    logger.info(f"Loaded calendar data: {calendar_raw.count()} records")
    return calendar_raw

def store_delta_tables(customers_df, transactions_df, calendar_df, raw_data_path):
    """Store raw data in Delta Lake format"""
    try:
        # Store customers
        customers_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{raw_data_path}customers")
        
        # Store transactions
        transactions_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{raw_data_path}transactions")
        
        # Store calendar
        calendar_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{raw_data_path}calendar")
        
        logger.info("All raw data stored in Delta Lake successfully")
        
    except Exception as e:
        logger.error(f"Error storing data: {str(e)}")
        raise

def main():
    """Main ingestion process"""
    # Configuration
    s3_bucket = os.getenv('S3_BUCKET')
    raw_data_path = os.getenv('RAW_DATA_PATH')
    
    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session initialized")
    
    try:
        # Load data
        customers_df = load_customers(spark, s3_bucket)
        transactions_df = load_transactions(spark, s3_bucket)
        calendar_df = load_calendar(spark, s3_bucket)
        
        # Store in Delta Lake
        store_delta_tables(customers_df, transactions_df, calendar_df, raw_data_path)
        
        # Simple validation
        logger.info(f"Final counts - Customers: {customers_df.count()}, Transactions: {transactions_df.count()}, Calendar: {calendar_df.count()}")
        
    except Exception as e:
        logger.error(f"Ingestion failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 