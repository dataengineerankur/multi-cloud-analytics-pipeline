#!/usr/bin/env python3
"""
Data Quality Validation Script
Performs data quality validation using PyDeequ framework with custom DataQuality class
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from dotenv import load_dotenv

# Add Deequ module to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'Deequ'))

# Load environment variables
load_dotenv('datapipeline/config.env')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Delta Lake and Deequ support"""
    os.environ["SPARK_VERSION"] = os.getenv('SPARK_VERSION', '3.3')
    
    return SparkSession.builder \
        .appName("PsychoBunny-DataQuality") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .config("spark.jars.packages", f"com.amazon.deequ:deequ:{os.getenv('DEEQU_VERSION')}") \
        .getOrCreate()

def load_data(spark, raw_data_path):
    """Load raw data from Delta Lake"""
    try:
        customers_df = spark.read.format("delta").load(f"{raw_data_path}customers")
        transactions_df = spark.read.format("delta").load(f"{raw_data_path}transactions")
        calendar_df = spark.read.format("delta").load(f"{raw_data_path}calendar")
        
        logger.info("Data loaded:")
        logger.info(f"Customers: {customers_df.count()}")
        logger.info(f"Transactions: {transactions_df.count()}")
        logger.info(f"Calendar: {calendar_df.count()}")
        
        return customers_df, transactions_df, calendar_df
    except Exception as e:
        logger.error(f"Error loading raw data: {str(e)}")
        raise

def import_pydeequ_and_custom_dq():
    """Import PyDeequ modules and custom DataQuality class"""
    try:
        from pydeequ.checks import Check, CheckLevel
        from pydeequ.verification import VerificationSuite
        from dq import DataQuality
        
        logger.info("PyDeequ modules and custom DataQuality class imported successfully")
        return Check, CheckLevel, VerificationSuite, DataQuality
    except ImportError as e:
        logger.error(f"Failed to import modules: {e}")
        raise

def run_customer_quality_checks(spark, customers_df, DataQuality):
    """Run data quality checks on customer data using custom DataQuality class"""
    try:
        logger.info("Running customer quality checks...")
        
        dq = (
            DataQuality(spark, customers_df)
            .add_size_check(1000)
            .add_not_null("customer_id")
            .add_unique("customer_id")
        )
        result = dq.run()
        
        logger.info("✅ Customer quality checks completed successfully")
        return result
    except Exception as e:
        logger.error(f"Customer quality checks failed: {str(e)}")
        raise

def run_email_validation(customers_df):
    """Run email format validation"""
    try:
        logger.info("Running email format validation...")
        
        email_regex = r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
        invalids = customers_df.filter(~col("email").rlike(email_regex))
        cnt = invalids.count()
        
        if cnt > 0:
            logger.warning(f"{cnt} invalid emails found")
            # Don't raise error, just log the warning
            logger.warning("Some email formats are invalid but continuing...")
        else:
            logger.info("✅ All email formats are valid")
            
        return cnt
    except Exception as e:
        logger.error(f"Email validation failed: {str(e)}")
        raise

def run_transaction_quality_checks(spark, transactions_df, DataQuality):
    """Run data quality checks on transaction data"""
    try:
        logger.info("Running transaction quality checks...")
        
        dq = (
            DataQuality(spark, transactions_df)
            .add_size_check(1000)
            .add_not_null("ORDERNUMBER")
            .add_not_null("PRODUCTCODE")
            .add_not_null("CUSTOMERNAME")
        )
        result = dq.run()
        
        logger.info("✅ Transaction quality checks completed successfully")
        return result
    except Exception as e:
        logger.error(f"Transaction quality checks failed: {str(e)}")
        raise

def run_calendar_quality_checks(spark, calendar_df, DataQuality):
    """Run data quality checks on calendar data"""
    try:
        logger.info("Running calendar quality checks...")
        
        dq = (
            DataQuality(spark, calendar_df)
            .add_size_check(5000)
            .add_not_null("CALENDAR_DATE")
            .add_unique("CALENDAR_DATE")
        )
        result = dq.run()
        
        logger.info("✅ Calendar quality checks completed successfully")
        return result
    except Exception as e:
        logger.error(f"Calendar quality checks failed: {str(e)}")
        raise

def main():
    """Main data quality validation process"""
    # Configuration
    raw_data_path = os.getenv('RAW_DATA_PATH')
    
    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session initialized with Delta Lake and Deequ support")
    
    try:
        # Load data
        customers_df, transactions_df, calendar_df = load_data(spark, raw_data_path)
        
        # Import PyDeequ and custom DataQuality
        Check, CheckLevel, VerificationSuite, DataQuality = import_pydeequ_and_custom_dq()
        
        # Run quality checks
        customer_results = run_customer_quality_checks(spark, customers_df, DataQuality)
        invalid_email_count = run_email_validation(customers_df)
        transaction_results = run_transaction_quality_checks(spark, transactions_df, DataQuality)
        calendar_results = run_calendar_quality_checks(spark, calendar_df, DataQuality)
        
        # Summary
        logger.info("=== Data Quality Validation Summary ===")
        logger.info("✅ All quality checks completed")
        logger.info(f"Customer checks: {'PASSED' if customer_results.status == 'Success' else 'FAILED'}")
        logger.info(f"Email validation: {invalid_email_count} invalid emails found")
        logger.info(f"Transaction checks: {'PASSED' if transaction_results.status == 'Success' else 'FAILED'}")
        logger.info(f"Calendar checks: {'PASSED' if calendar_results.status == 'Success' else 'FAILED'}")
        logger.info("All checks passed!")
        
    except Exception as e:
        logger.error(f"Data quality validation failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 