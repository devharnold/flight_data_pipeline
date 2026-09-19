# SkyStream

## AWS Flight Data Engineering Pipeline

SkyStream is an end-to-end data engineering project that processes historical flight data using AWS cloud services.

The project is designed to demonstrate how raw aviation data can be ingested, stored in a cloud data lake, transformed into analytics-ready datasets, catalogued, and queried using serverless analytics services.

The first version focuses on **batch processing of historical flight data**. A future phase will extend the platform to support **incremental and near-real-time flight data ingestion**.

---

## Architecture

```text
                    Historical Flight Data
                            │
                            ▼
                     Python Ingestion
                            │
                            ▼
                       Amazon S3
                            │
                    ┌───────┴───────┐
                    │               │
                  Raw             Silver
                    │               │
                    └───────┬───────┘
                            ▼
                     AWS Glue / PySpark
                            │
                            ▼
                         Gold
                            │
                            ▼
                  Glue Data Catalog
                            │
                            ▼
                     Amazon Athena
                            │
                            ▼
                      SQL Analytics
```

Apache Airflow will be used to orchestrate the pipeline as the project develops.

---

## Objectives

The project focuses on building practical cloud data engineering skills around:

* Cloud-based data ingestion
* Data lake architecture
* Batch data processing
* Distributed data transformation with PySpark
* Data partitioning
* Parquet-based storage
* Data quality validation
* Workflow orchestration
* Metadata management
* Serverless SQL analytics
* Infrastructure as Code
* AWS security and IAM
* Monitoring and operational reliability

---

## Technology Stack

| Component         | Technology            |
| ----------------- | --------------------- |
| Programming       | Python                |
| Cloud             | AWS                   |
| Object Storage    | Amazon S3             |
| Data Processing   | AWS Glue / PySpark    |
| Query Engine      | AWS Redshift         |
| Metadata          | AWS Glue Data Catalog |
| Orchestration     | Apache Airflow        |
| Infrastructure    | Terraform             |
| Data Format       | Parquet               |
| Database Querying | SQL                   |
| Version Control   | Git / GitHub          |

---

## Data Pipeline

### 1. Ingestion

Historical flight data is retrieved from a public aviation dataset and loaded into Amazon S3.

The original data is preserved in the **raw layer** so that the pipeline can be reprocessed without repeatedly retrieving the source data.

```text
Source Dataset
      │
      ▼
Python Ingestion
      │
      ▼
S3 Raw Layer
```

### 2. Raw Data Lake

Raw files are stored in S3 without modifying the original records.

Example:

```text
s3://skystream-flight-data/
└── raw/
    └── flights/
        └── ingestion_date=YYYY-MM-DD/
            └── flights.csv
```

---

### 3. Transformation

AWS Glue and PySpark process the raw data.

Transformations will include:

* Schema normalization
* Data type conversion
* Timestamp standardization
* Duplicate detection
* Missing-value handling
* Invalid-record filtering
* Airport and airline normalization
* Derived metrics
* Data partitioning

The transformed data will be stored as **Parquet**.

```text
S3 Raw
   │
   ▼
AWS Glue / PySpark
   │
   ▼
S3 Silver
```

---

### 4. Curated Data

The pipeline produces final data marts that are analytics-ready.

Example datasets:

```text
marts/
├── daily_flight_summary/
├── airline_performance/
├── airport_traffic/
├── route_statistics/
└── flight_delays/
```

These datasets are designed for analytical workloads rather than operational transactions.

---

### 5. Data Catalog

AWS Glue Data Catalog will maintain metadata describing the datasets stored in S3.

This allows analytical tools such as Redshift to understand the structure of the data without requiring the datasets to be loaded into a traditional database.

---

### 6. Analytics

Aws Redshift will be used to query the curated datasets directly from S3 using SQL.

Example analytical questions include:

```sql
-- Which airlines operate the most flights?

SELECT
    airline,
    COUNT(*) AS total_flights
FROM flights
GROUP BY airline
ORDER BY total_flights DESC;
```

Other questions the pipeline will support include:

* Which airports have the highest traffic?
* Which routes have the most flights?
* Which airlines have the highest average delays?
* How does flight volume change by month?
* What percentage of flights are delayed?
* Which airports experience the greatest departure delays?
* How do delays vary by time of day?

---

## Data Lake Structure

The S3 data lake follows a layered architecture:

```text
skystream-flight-data/
│
├── raw/
│   └── flights/
│
├── staging/
│   ├── flights/
│   ├── airports/
│   └── airlines/
│
└── marts/
    ├── daily_flight_summary/
    ├── airline_performance/
    ├── airport_traffic/
    ├── route_statistics/
    └── flight_delays/
```

### Raw

Original source data.

### Staging

Cleaned and standardized datasets.

### Marts

Business and analytics-ready datasets.

---

## Partitioning

Datasets will be partitioned to improve query performance and reduce unnecessary data scanned by Redshift.

Example:

```text
staging/flights/
├── year=2024/
│   ├── month=01/
│   ├── month=02/
│   └── month=03/
│
└── year=2025/
    ├── month=01/
    ├── month=02/
    └── month=03/
```

Partitioning strategy will be evaluated based on the actual query patterns and dataset characteristics rather than added merely because every tutorial on the internet apparently requires twelve partitions and a prayer.

---

## Data Quality

The pipeline will include validation checks for:

* Required columns
* Null values
* Duplicate records
* Invalid airport codes
* Invalid timestamps
* Invalid flight durations
* Invalid delay values
* Unexpected schema changes
* Record counts between pipeline stages

A failed validation should prevent invalid data from progressing into the curated layer.

---

## Orchestration

Apache Airflow will orchestrate the pipeline.

A simplified DAG:

```text
start
  │
  ▼
ingest_flight_data
  │
  ▼
validate_raw_data
  │
  ▼
transform_flight_data
  │
  ▼
validate_transformed_data
  │
  ▼
update_catalog
  │
  ▼
pipeline_complete
```

The pipeline should be **idempotent**, meaning rerunning a task should not unintentionally create duplicate data.

---

## Infrastructure as Code

Terraform will be used to manage AWS infrastructure.

Planned resources include:

* S3 buckets
* IAM roles
* IAM policies
* AWS Glue resources
* Redshift Config
* CloudWatch resources

The goal is to make the infrastructure reproducible rather than manually creating everything through the AWS console.

---

## Security

The project will follow basic AWS security practices:

* IAM roles instead of hard-coded credentials
* Least-privilege permissions
* No AWS credentials committed to Git
* Environment variables for local configuration
* `.gitignore` for secrets and local configuration
* Separate development and production resources where appropriate

---

## Project Phases

### Phase 1: Historical Batch Pipeline

* Select flight dataset
* Explore dataset and schema
* Create AWS account resources
* Create S3 data lake
* Build Python ingestion
* Upload raw data to S3
* Implement data validation
* Build PySpark transformations
* Convert data to Parquet
* Create staging layer
* Come up with final data marts
* Configure Glue Data Catalog
* Query data with Redshift
* Add Airflow orchestration
* Add Terraform
* Add monitoring and logging
* Document architecture and results

### Phase 2: Incremental Flight Pipeline

The second phase will extend SkyStream beyond historical batch processing.

Planned improvements:

* Integrate a flight/aviation API
* Implement incremental ingestion
* Handle API pagination
* Handle rate limits
* Track ingestion state
* Process newly received flight records
* Handle changing flight status
* Improve monitoring
* Add automated data quality checks

### Phase 3: Advanced Processing

Potential future extensions:

* Larger-scale PySpark workloads
* Streaming ingestion
* Apache Kafka
* AWS streaming services
* Near-real-time analytics
* Flight delay dashboard
* Cost and performance optimization

---

## Example Architecture Evolution

SkyStream is intentionally designed to evolve.

### Version 1

```text
Historical Dataset
       ↓
Python
       ↓
S3
       ↓
Glue / PySpark
       ↓
Redshift
```

### Version 2

```text
Historical + API Data
          ↓
       Airflow
          ↓
         S3
          ↓
   Glue / PySpark
          ↓
      Data Lake
          ↓
        Redshift
```

### Future Version

```text
Flight Events
     ↓
Streaming Ingestion
     ↓
Kafka / AWS Streaming
     ↓
Stream Processing
     ↓
S3 / Analytics Store
     ↓
Real-Time Analytics
```

---

## Project Outcomes

By completing SkyStream, the project will demonstrate practical experience with:

**AWS**

* S3
* Glue
* Redshift
* IAM
* CloudWatch

**Data Engineering**

* Batch ingestion
* Incremental ingestion
* ETL/ELT
* Data lakes
* Data partitioning
* Data quality
* Distributed processing
* Workflow orchestration

**Engineering**

* Python
* SQL
* PySpark
* Terraform
* Docker
* Git

---

## Status

**In Development**

Current focus: **Historical flight data pipeline on AWS**

Future focus: **Incremental and near-real-time flight data processing**
