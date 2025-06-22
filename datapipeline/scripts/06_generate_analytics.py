#!/usr/bin/env python3
"""
Analytics Generation Script
Creates analytics tables based on processed data
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from dotenv import load_dotenv

# Load environment variables
load_dotenv('datapipeline/config.env')

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create Spark session with Delta Lake and S3 support"""
    return SparkSession.builder \
        .appName("PsychoBunny-Analytics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()

def load_processed_data(spark, processed_data_path, raw_data_path):
    """Load processed data from Delta Lake"""
    try:
        fact_transactions = spark.read.format("delta").load(f"{processed_data_path}fact_transactions")
        dim_customer = spark.read.format("delta").load(f"{processed_data_path}dim_customer")
        dim_product = spark.read.format("delta").load(f"{processed_data_path}dim_product")
        calendar_df = spark.read.format("delta").load(f"{raw_data_path}calendar")
        
        logger.info(f"✅ Loaded data: {fact_transactions.count()} transactions, {dim_customer.count()} customers")
        return fact_transactions, dim_customer, dim_product, calendar_df
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise

def create_sales_analytics(fact_transactions):
    """Weekly, Monthly, Quarterly Sales Analysis"""
    sales_analytics = fact_transactions.filter(col("transaction_type") == "SALE") \
        .withColumn("week", weekofyear(col("order_date"))) \
        .withColumn("month", month(col("order_date"))) \
        .withColumn("quarter", quarter(col("order_date"))) \
        .withColumn("year", year(col("order_date"))) \
        .groupBy("year", "quarter", "month", "week") \
        .agg(
            sum("net_amount").alias("total_sales"),
            count("order_number").alias("total_orders"),
            avg("net_amount").alias("avg_order_value")
        ).withColumn("created_date", current_timestamp())

    logger.info(f"✅ Sales Analytics: {sales_analytics.count()} records")
    return sales_analytics

def create_refunds_analytics(fact_transactions):
    """Weekly, Monthly, Quarterly Refunds Analysis"""
    refunds_analytics = fact_transactions.filter(col("transaction_type") == "REFUND") \
        .withColumn("week", weekofyear(col("order_date"))) \
        .withColumn("month", month(col("order_date"))) \
        .withColumn("quarter", quarter(col("order_date"))) \
        .withColumn("year", year(col("order_date"))) \
        .groupBy("year", "quarter", "month", "week") \
        .agg(
            sum("net_amount").alias("total_refunds"),
            sum("restocking_fee").alias("total_restocking_fees"),
            count("order_number").alias("total_refund_orders")
        ).withColumn("created_date", current_timestamp())

    logger.info(f"✅ Refunds Analytics: {refunds_analytics.count()} records")
    return refunds_analytics

def create_product_family_sales(fact_transactions):
    """Product Family Analysis"""
    def extract_product_family(product_code):
        if product_code and "_" in product_code:
            return product_code.split("_")[0]
        return "UNKNOWN"

    extract_family_udf = udf(extract_product_family, StringType())

    product_family_sales = fact_transactions.filter(col("transaction_type") == "SALE") \
        .withColumn("product_family", extract_family_udf(col("product_code"))) \
        .groupBy("product_family") \
        .agg(
            sum("net_amount").alias("total_sales"),
            count("order_number").alias("total_orders"),
            avg("net_amount").alias("avg_order_value")
        ).orderBy(desc("total_sales")) \
        .withColumn("created_date", current_timestamp())

    logger.info(f"✅ Product Family Sales: {product_family_sales.count()} families")
    return product_family_sales

def create_regional_rankings(fact_transactions):
    """Best/Second Best Items by Region"""
    def extract_product_family(product_code):
        if product_code and "_" in product_code:
            return product_code.split("_")[0]
        return "UNKNOWN"

    extract_family_udf = udf(extract_product_family, StringType())

    # Calculate product sales by territory
    product_territory_sales = fact_transactions.filter(col("transaction_type") == "SALE") \
        .withColumn("product_family", extract_family_udf(col("product_code"))) \
        .groupBy("territory", "product_code", "product_family") \
        .agg(
            sum("net_amount").alias("total_sales"),
            count("order_number").alias("total_orders")
        )

    # Add ranking within each territory
    window_spec = Window.partitionBy("territory").orderBy(desc("total_sales"))
    
    regional_rankings = product_territory_sales \
        .withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") <= 2) \
        .withColumn("ranking_type", 
                   when(col("rank") == 1, "Best Selling")
                   .when(col("rank") == 2, "Second Best Selling")) \
        .withColumn("created_date", current_timestamp())

    logger.info(f"✅ Regional Rankings: {regional_rankings.count()} records")
    return regional_rankings

def create_enhanced_customer_segments(fact_transactions, dim_customer):
    """Enhanced Customer Segmentation"""
    # Calculate customer metrics
    customer_metrics = fact_transactions.filter(col("transaction_type") == "SALE") \
        .groupBy("customer_name") \
        .agg(
            sum("net_amount").alias("total_spent"),
            count("order_number").alias("total_orders"),
            avg("net_amount").alias("avg_order_value"),
            max("order_date").alias("last_order_date"),
            countDistinct("product_code").alias("unique_products_purchased")
        )

    # Calculate percentiles for segmentation
    percentiles = customer_metrics.select(
        expr("percentile_approx(total_spent, 0.33)").alias("p33"),
        expr("percentile_approx(total_spent, 0.67)").alias("p67")
    ).collect()[0]

    enhanced_customer_segments = customer_metrics \
        .withColumn("customer_segment",
                   when(col("total_spent") >= percentiles.p67, "High Value")
                   .when(col("total_spent") >= percentiles.p33, "Medium Value")
                   .otherwise("Low Value")) \
        .withColumn("created_date", current_timestamp())

    logger.info(f"✅ Enhanced Customer Segments: {enhanced_customer_segments.count()} customers")
    return enhanced_customer_segments

def create_daily_insights(fact_transactions):
    """Daily Insights for BI Dashboards"""
    daily_insights = fact_transactions.filter(col("transaction_type") == "SALE") \
        .withColumn("order_date_str", date_format(col("order_date"), "yyyy-MM-dd")) \
        .groupBy("order_date_str") \
        .agg(
            sum("net_amount").alias("daily_sales"),
            count("order_number").alias("daily_orders"),
            avg("net_amount").alias("daily_avg_order"),
            countDistinct("customer_name").alias("unique_customers"),
            countDistinct("product_code").alias("unique_products")
        )

    # Calculate percentiles for categorization
    percentiles = daily_insights.select(
        expr("percentile_approx(daily_sales, 0.33)").alias("p33"),
        expr("percentile_approx(daily_sales, 0.67)").alias("p67")
    ).collect()[0]

    daily_insights = daily_insights \
        .withColumn("sales_category",
                   when(col("daily_sales") >= percentiles.p67, "High Sales Day")
                   .when(col("daily_sales") >= percentiles.p33, "Medium Sales Day")
                   .otherwise("Low Sales Day")) \
        .withColumn("created_date", current_timestamp())

    logger.info(f"✅ Daily Insights: {daily_insights.count()} days")
    return daily_insights

def store_analytics_tables(analytics_tables, processed_data_path):
    """Store analytics tables in Delta Lake"""
    try:
        for table_name, df in analytics_tables.items():
            df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(f"{processed_data_path}analytics_{table_name}")
            logger.info(f"✅ Stored {table_name} with {df.count()} records")
        
        logger.info("✅ All analytics tables stored successfully")
        
    except Exception as e:
        logger.error(f"❌ Error storing analytics tables: {str(e)}")
        raise

def main():
    """Main analytics generation process"""
    # Configuration
    processed_data_path = os.getenv('PROCESSED_DATA_PATH')
    raw_data_path = os.getenv('RAW_DATA_PATH')
    
    # Create Spark session
    spark = create_spark_session()
    logger.info("Spark session initialized")
    
    try:
        # Load processed data
        fact_transactions, dim_customer, dim_product, calendar_df = load_processed_data(spark, processed_data_path, raw_data_path)
        
        # Generate analytics tables
        sales_analytics = create_sales_analytics(fact_transactions)
        refunds_analytics = create_refunds_analytics(fact_transactions)
        product_family_sales = create_product_family_sales(fact_transactions)
        regional_rankings = create_regional_rankings(fact_transactions)
        enhanced_customer_segments = create_enhanced_customer_segments(fact_transactions, dim_customer)
        daily_insights = create_daily_insights(fact_transactions)
        
        # Store analytics tables
        analytics_tables = {
            "sales_analytics": sales_analytics,
            "refunds_analytics": refunds_analytics,
            "product_family_sales": product_family_sales,
            "regional_rankings": regional_rankings,
            "enhanced_customer_segments": enhanced_customer_segments,
            "daily_insights": daily_insights
        }
        
        store_analytics_tables(analytics_tables, processed_data_path)
        
        # Summary
        logger.info("=== Analytics Generation Summary ===")
        for table_name, df in analytics_tables.items():
            logger.info(f"✅ {table_name}: {df.count()} records")
        
    except Exception as e:
        logger.error(f"Analytics generation failed: {str(e)}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main() 