#!/usr/bin/env python3
"""
Load to Redshift Script
Loads processed data from Delta Lake to Redshift
"""

import os
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from dotenv import load_dotenv

# Load environment variables
load_dotenv('datapipeline/config.env')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Delta Lake and S3 support"""
    return SparkSession.builder \
        .appName("PsychoBunny-RedshiftLoad") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def get_redshift_config():
    """Get Redshift connection configuration"""
    return {
        "host": os.getenv('REDSHIFT_HOST'),
        "port": os.getenv('REDSHIFT_PORT'),
        "database": os.getenv('REDSHIFT_DATABASE'),
        "user": os.getenv('REDSHIFT_USER'),
        "password": os.getenv('REDSHIFT_PASSWORD')
    }

def create_redshift_tables(redshift_config):
    """Create Redshift tables if they don't exist"""
    create_table_sql = {
        "dim_customer": """
            CREATE TABLE IF NOT EXISTS dim_customer (
                customer_key VARCHAR(500),
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                full_name VARCHAR(200),
                email VARCHAR(200),
                company_name VARCHAR(200),
                address VARCHAR(500),
                city VARCHAR(100),
                state VARCHAR(50),
                postal VARCHAR(20),
                province VARCHAR(50),
                phone VARCHAR(50),
                created_date TIMESTAMP
            );
        """,
        "dim_product": """
            CREATE TABLE IF NOT EXISTS dim_product (
                PRODUCTCODE VARCHAR(50),
                product_key VARCHAR(50),
                product_family VARCHAR(100),
                product_line VARCHAR(100),
                created_date TIMESTAMP
            );
        """,
        "fact_transactions": """
            CREATE TABLE IF NOT EXISTS fact_transactions (
                order_number VARCHAR(50),
                customer_name VARCHAR(200),
                product_code VARCHAR(50),
                quantity INTEGER,
                unit_price DECIMAL(10,2),
                total_amount DECIMAL(12,2),
                transaction_type VARCHAR(20),
                restocking_fee DECIMAL(10,2),
                net_amount DECIMAL(12,2),
                is_large_order BOOLEAN,
                order_date DATE,
                territory VARCHAR(50),
                product_line VARCHAR(100),
                status VARCHAR(50),
                created_date TIMESTAMP
            );
        """,
        "customer_segments": """
            CREATE TABLE IF NOT EXISTS customer_segments (
                customer_name VARCHAR(200),
                total_spent DECIMAL(12,2),
                total_orders INTEGER,
                avg_order_value DECIMAL(10,2),
                last_order_date DATE,
                customer_segment VARCHAR(50),
                created_date TIMESTAMP
            );
        """
    }
    
    try:
        conn = psycopg2.connect(**redshift_config)
        cursor = conn.cursor()
        
        for table_name, sql in create_table_sql.items():
            logger.info(f"Creating table: {table_name}")
            cursor.execute(sql)
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("✅ All Redshift tables created successfully")
        
    except Exception as e:
        logger.error(f"❌ Error creating Redshift tables: {str(e)}")
        raise

def load_to_redshift(df, table_name, redshift_config):
    """Load DataFrame to Redshift"""
    try:
        pandas_df = df.toPandas()
        
        conn = psycopg2.connect(**redshift_config)
        cursor = conn.cursor()
        
        # Clear existing data
        cursor.execute(f"DELETE FROM {table_name};")
        
        # Insert new data
        for _, row in pandas_df.iterrows():
            placeholders = ', '.join(['%s'] * len(row))
            columns = ', '.join(row.index)
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, tuple(row))
        
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"✅ Loaded {len(pandas_df)} records to {table_name}")
        
    except Exception as e:
        logger.error(f"❌ Error loading {table_name}: {str(e)}")
        raise

def load_processed_data(spark, processed_data_path):
    """Load processed data from Delta Lake"""
    try:
        dim_customer = spark.read.format("delta").load(f"{processed_data_path}dim_customer")
        dim_product = spark.read.format("delta").load(f"{processed_data_path}dim_product")
        fact_transactions = spark.read.format("delta").load(f"{processed_data_path}fact_transactions")
        customer_segments = spark.read.format("delta").load(f"{processed_data_path}customer_segments")
        
        logger.info("✅ Processed data loaded from Delta Lake")
        return dim_customer, dim_product, fact_transactions, customer_segments
    except Exception as e:
        logger.error(f"Error loading processed data: {str(e)}")
        raise

def main():
    """Main Redshift loading process"""
    # Configuration
    processed_data_path = os.getenv('PROCESSED_DATA_PATH')
    redshift_config = get_redshift_config()
    
    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session initialized")
    
    try:
        # Create Redshift tables
        create_redshift_tables(redshift_config)
        
        # Load processed data
        dim_customer, dim_product, fact_transactions, customer_segments = load_processed_data(spark, processed_data_path)
        
        # Load to Redshift
        load_to_redshift(dim_customer, "dim_customer", redshift_config)
        load_to_redshift(dim_product, "dim_product", redshift_config)
        load_to_redshift(fact_transactions, "fact_transactions", redshift_config)
        load_to_redshift(customer_segments, "customer_segments", redshift_config)
        
        # Summary
        logger.info("=== Redshift Load Summary ===")
        logger.info(f"✅ dim_customer: {dim_customer.count()} records")
        logger.info(f"✅ dim_product: {dim_product.count()} records")
        logger.info(f"✅ fact_transactions: {fact_transactions.count()} records")
        logger.info(f"✅ customer_segments: {customer_segments.count()} records")
        
    except Exception as e:
        logger.error(f"Redshift loading failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 