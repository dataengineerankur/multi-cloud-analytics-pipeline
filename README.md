# Psycho Bunny Data Pipeline Project

## Project Description
This project develops a robust, scalable, and automated data pipeline to process and analyze customer and transaction data for Psycho Bunny. The pipeline ingests data from multiple sources, performs data quality checks using AWS Deequ, processes data using PySpark, and stores results in Delta Lake format before loading into Redshift and Snowflake data warehouses for different analytical purposes.

## Architecture Overview
- **Landing Zone**: AWS S3 for raw data storage
- **Processing Layer**: Apache Spark for ETL/ELT operations
- **Data Quality**: AWS Deequ framework for automated data validation
- **Data Warehouses**:
  - Delta Lake: Primary analytical storage with Schema Evolution & ACID compliance
  - Redshift for operational analytics and reporting
  - Snowflake for advanced analytics and data science workloads
- **Orchestration**: Apache Airflow for workflow management
- **Monitoring**: AWS CloudWatch and Airflow UI

## How to Use This Repository

### Structure
- `data/`: Contains raw data files (customers, transactions, fiscal calendar)
- `notebooks/`: Databricks notebooks for step-by-step data processing
- `scripts/`: Python scripts for automated data processing and orchestration
- `config/`: Configuration files for database connections and AWS services
- `lib/`: Helper libraries for Spark session setup, Deequ integration, and utilities
- [`docs/`](https://github.com/innovacraft/psycho_bunny_pipeline/blob/main/docs/project_documentation.md): Project documentation, ERD diagrams, and architecture documentation
- `airflow/`: Airflow DAGs, configuration, and logs for orchestrating the pipeline
- `terraform/`: Infrastructure as Code for AWS resources provisioning (Future Segment)
- `deequ/`: Data quality tests and validation scripts using AWS Deequ

### Prerequisites
1. AWS Account with appropriate permissions for S3, IAM, CloudWatch, and Redshift
2. Databricks workspace (or Spark environment setup on EMR / Local) with access to AWS S3
3. Snowflake account and credentials
4. Python 3.8+ with required packages
5. Apache Airflow (local or cloud deployment)

### Setup
1. **AWS Configuration**:
   - Configure AWS credentials with access to S3, IAM, CloudWatch, and Redshift
   - Set up S3 buckets for landing zone and processed data
   - Configure IAM roles for Databricks and Airflow

2. **Databricks or Spark(On EMR/Local) Setup**:
   - Install required libraries: `pyspark`, `delta`, `boto3`, `pydeequ`
   - Configure cluster with appropriate instance types
   - Set up S3 access from Databricks (Or EMR/Local)

3. **Data Warehouse Setup**:
   - Configure Redshift cluster and create target schemas
   - Set up Snowflake account and create target databases/schemas
   - Configure connection parameters in `config/` directory

4. **Airflow Setup**:
   - Install Airflow and required dependencies
   - Configure connections to AWS, Databricks, Redshift, and Snowflake
   - Set up DAGs for pipeline orchestration

### Running the Pipeline

#### 1. Data Ingestion
Execute the ingestion scripts to load data from raw sources:
```bash
python scripts/01_data_ingestion.py
```

#### 2. Data Quality Validation
Run data quality checks using AWS Deequ:
```bash
python scripts/02_data_quality_validation.py
```

#### 3. Data Processing
Execute the main processing pipeline:
```bash
python scripts/03_data_processing.py
```

#### 4. Data Warehouse Loading
Load processed data into data warehouses:
```bash
python scripts/04_load_to_redshift.py
python scripts/05_load_to_snowflake.py
```

#### 5. Analytics and Reporting
Generate analytics and reports:
```bash
python scripts/06_generate_analytics.py
```

### Monitoring and Maintenance
- Monitor pipeline execution using Airflow UI
- Check data quality metrics in AWS CloudWatch & Datadog (Future Segment)
- Review logs in S3 and CloudWatch
- Set up alerts for pipeline failures and data quality issues

### Data Quality Framework
The pipeline uses AWS Deequ for automated data quality validation:
- Completeness checks for required fields
- Uniqueness validation for primary keys
- Referential integrity validation
- Statistical anomaly detection
- Custom business rule validation

### Analytics and Metrics
The pipeline generates the following key metrics:
- Weekly, monthly, quarterly sales and refunds
- Total items sold by product family
- Best/second-best selling items per region
- Revenue difference per item vs. best in region
- Customer segmentation (High/Medium/Low value)
- Top 10 spending customers with contact details
- Refund restocking fee calculations (10%)

## Contributing
This project is developed as a Senior Data Engineer assessment for Psycho Bunny. The repository demonstrates enterprise-grade data pipeline capabilities including:
- Scalable data processing with Apache Spark
- Automated data quality validation
- Multi-cloud data warehouse integration
- Infrastructure as Code practices
- Comprehensive monitoring and alerting

## License
This project is developed for educational and demonstrative purposes as part of the Psycho Bunny Senior Data Engineer assessment. 
