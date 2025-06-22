#!/usr/bin/env python3
"""
Data Processing Script
Transforms raw data into processed dimensions and fact tables
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
        .appName("PsychoBunny-DataProcessing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def load_raw_data(spark, raw_data_path):
    """Load raw data from Delta Lake"""
    try:
        customers_df = spark.read.format("delta").load(f"{raw_data_path}customers")
        transactions_df = spark.read.format("delta").load(f"{raw_data_path}transactions")
        calendar_df = spark.read.format("delta").load(f"{raw_data_path}calendar")
        
        logger.info("✅ Raw data loaded successfully")
        logger.info(f"Customers: {customers_df.count()}, Transactions: {transactions_df.count()}, Calendar: {calendar_df.count()}")
        
        return customers_df, transactions_df, calendar_df
    except Exception as e:
        logger.error(f"Error loading raw data: {str(e)}")
        raise

def create_customer_dimension(customers_df):
    """Create customer dimension table"""
    dim_customer = customers_df.select(
        col("customer_id").alias("customer_key"),
        col("first_name"),
        col("last_name"),
        concat_ws(" ", col("first_name"), col("last_name")).alias("full_name"),
        col("email"),
        col("company_name"),
        col("address"),
        col("city"),
        col("state"),
        col("postal"),
        col("province"),
        col("phone1").alias("phone"),
        col("ingestion_date").alias("created_date")
    ).dropDuplicates(["customer_key"])

    logger.info(f"✅ Created customer dimension with {dim_customer.count()} records")
    return dim_customer

def create_product_dimension(transactions_df):
    """Create product dimension table"""
    def extract_product_family(product_code):
        if product_code and "_" in product_code:
            return product_code.split("_")[0]
        return "UNKNOWN"

    extract_product_family_udf = udf(extract_product_family, StringType())

    dim_product = transactions_df.select("PRODUCTCODE").distinct() \
        .withColumn("product_key", col("PRODUCTCODE")) \
        .withColumn("product_family", extract_product_family_udf(col("PRODUCTCODE"))) \
        .withColumn("product_line", lit("General")) \
        .withColumn("created_date", current_timestamp())

    logger.info(f"✅ Created product dimension with {dim_product.count()} records")
    return dim_product

def create_fact_transactions(transactions_df):
    """Create fact transactions table with business rules"""
    # Business rules
    BUSINESS_RULES = {
        "high_value_customer_threshold": int(os.getenv('HIGH_VALUE_CUSTOMER_THRESHOLD', 10000)),
        "refund_restocking_fee_rate": float(os.getenv('REFUND_RESTOCKING_FEE_RATE', 0.10)),
        "large_order_threshold": int(os.getenv('LARGE_ORDER_THRESHOLD', 5000))
    }

    fact_transactions = transactions_df.select(
        col("ORDERNUMBER").alias("order_number"),
        col("CUSTOMERNAME").alias("customer_name"),
        col("PRODUCTCODE").alias("product_code"),
        col("QUANTITYORDERED").alias("quantity"),
        (abs(col("TOTAL_AMOUNT")) / col("QUANTITYORDERED")).alias("unit_price"),
        abs(col("TOTAL_AMOUNT")).alias("total_amount"),
        col("DEALSIZE").alias("deal_size"),
        col("TERRITORY").alias("territory"),
        to_date(col("ORDERDATE"), "M/d/yyyy H:mm").alias("order_date"),
        lit("COMPLETED").alias("status"),
        lit("General").alias("product_line"),
        current_timestamp().alias("created_date")
    ).withColumn(
        "transaction_type",
        when(col("TOTAL_AMOUNT") < 0, "REFUND").otherwise("SALE")
    ).withColumn(
        "restocking_fee", 
        when(col("transaction_type") == "REFUND", 
             abs(col("TOTAL_AMOUNT")) * BUSINESS_RULES["refund_restocking_fee_rate"])
        .otherwise(0.0)
    ).withColumn(
        "net_amount",
        when(col("transaction_type") == "REFUND", 
             abs(col("TOTAL_AMOUNT")) - col("restocking_fee"))
        .otherwise(abs(col("TOTAL_AMOUNT")))
    ).withColumn(
        "is_large_order",
        when(abs(col("TOTAL_AMOUNT")) >= BUSINESS_RULES["large_order_threshold"], true)
        .otherwise(false)
    )

    logger.info(f"✅ Created fact transactions with {fact_transactions.count()} records")
    return fact_transactions

def create_customer_segments(fact_transactions):
    """Create customer segmentation table"""
    HIGH_VALUE_THRESHOLD = int(os.getenv('HIGH_VALUE_CUSTOMER_THRESHOLD', 10000))
    
    customer_segments = fact_transactions.filter(col("transaction_type") == "SALE") \
        .groupBy("customer_name") \
        .agg(
            sum("net_amount").alias("total_spent"),
            count("order_number").alias("total_orders"),
            avg("net_amount").alias("avg_order_value"),
            max("order_date").alias("last_order_date")
        ).withColumn(
            "customer_segment",
            when(col("total_spent") >= HIGH_VALUE_THRESHOLD, "High Value")
            .otherwise("Regular")
        ).withColumn("created_date", current_timestamp())

    logger.info(f"✅ Created customer segments with {customer_segments.count()} customers")
    return customer_segments

def store_processed_data(dim_customer, dim_product, fact_transactions, customer_segments, processed_data_path):
    """Store processed data in Delta Lake"""
    try:
        dim_customer.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{processed_data_path}dim_customer")
        
        dim_product.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{processed_data_path}dim_product")
        
        fact_transactions.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{processed_data_path}fact_transactions")
        
        customer_segments.write \
            .format("delta") \
            .mode("overwrite") \
            .save(f"{processed_data_path}customer_segments")
        
        logger.info("✅ All processed data stored in Delta Lake successfully")
        
    except Exception as e:
        logger.error(f"❌ Error storing processed data: {str(e)}")
        raise

def main():
    """Main data processing pipeline"""
    # Configuration
    raw_data_path = os.getenv('RAW_DATA_PATH')
    processed_data_path = os.getenv('PROCESSED_DATA_PATH')
    
    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session initialized")
    
    try:
        # Load raw data
        customers_df, transactions_df, calendar_df = load_raw_data(spark, raw_data_path)
        
        # Create dimensions
        dim_customer = create_customer_dimension(customers_df)
        dim_product = create_product_dimension(transactions_df)
        
        # Create fact table
        fact_transactions = create_fact_transactions(transactions_df)
        
        # Create analytics tables
        customer_segments = create_customer_segments(fact_transactions)
        
        # Store processed data
        store_processed_data(dim_customer, dim_product, fact_transactions, customer_segments, processed_data_path)
        
        # Summary
        logger.info("=== Data Processing Summary ===")
        logger.info(f"Customer dimension: {dim_customer.count()} records")
        logger.info(f"Product dimension: {dim_product.count()} records")
        logger.info(f"Fact transactions: {fact_transactions.count()} records")
        logger.info(f"Customer segments: {customer_segments.count()} records")
        
    except Exception as e:
        logger.error(f"Data processing failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 