# Lakehouse Architecture for E-Commerce Transactions

## Project Summary

This project implements a **production-grade Lakehouse architecture** on **AWS** for managing **e-commerce transactional data**. It:

- Ingests raw CSV files from **Amazon S3**
- Processes data using **AWS Glue** with **Apache Spark** and **Delta Lake**
- Stores curated data in a partitioned Delta format
- Makes data queryable via **Amazon Athena**
- Orchestrates the ETL workflow using **AWS Step Functions**
- Automates deployment and validation using **GitHub Actions**

---

## Architecture Overview

### Lakehouse Zones (Amazon S3)

| Zone           | Purpose                                                                 |
|----------------|-------------------------------------------------------------------------|
| **Raw Zone**   | Landing area for raw incoming `.csv` files (`products`, `orders`, `order_items`) |
| **Processed Zone** | Cleaned, validated, deduplicated Delta Lake tables (partitioned by `department` (products) and `date` (orders & order_items)) |
| **Archive Zone**   | Stores ingested raw files post-processing for auditing/backups        |

### Processing Layer

- **AWS Glue** jobs with **Apache Spark**
- Uses **Delta Lake** for:
  - ACID compliance
  - Schema enforcement
  - Deduplication via `MERGE`
- Writes to partitioned Delta tables in the **Processed Zone**

### Metadata & Query Layer

- **AWS Glue Data Catalog** manages table metadata and schema evolution
- **Amazon Athena** allows SQL querying over partitioned Delta tables

### Orchestration Layer

| Component       | Role                                                                 |
|----------------|----------------------------------------------------------------------|
| **Lambada Function** | Detects `.csv` uploads in Raw Zone and triggers Step Function        |
| **Step Functions** | Manages ETL pipeline execution: Glue → Lambda → Crawler → Athena |


### CI/CD & Automation

| Component        | Role                                                                 |
|------------------|----------------------------------------------------------------------|
| **GitHub Actions** | Automates testing, validation, and deployment of Glue & orchestration scripts |
| **Python Unit Tests** | Validate transformation logic in Glue scripts                    |
| **Linting & Code Quality** | Ensure maintainable and production-safe code                |

---

## Architecture Diagram



---

## Key Features

- **Automated Ingestion** via Lambda → Step Function → Glue
- **Spark-based ETL** with typed schema enforcement and data validation
- **Deduplication** using Delta Lake’s `MERGE INTO` semantics
- **Delta Lake** for ACID transactions, time travel, schema evolution
- **Partitioning** improves query speed (e.g., `date` partition)
- **Athena-ready Format** using Delta-to-Parquet compatibility
- **CI/CD Integration** using GitHub Actions for code deployment
- **Validation Logic** in SQL (Athena) and unit tests (Pytest)
- **Archiving Strategy** to offload raw files post-processing

---

## Technology Stack

| Component       | Technology/Service        |
|-----------------|---------------------------|
| **Storage**     | Amazon S3                 |
| **ETL**         | AWS Glue, Apache Spark    |
| **Table Format**| Delta Lake                |
| **Metadata**    | AWS Glue Data Catalog     |
| **Query Engine**| Amazon Athena             |
| **Orchestration**| AWS Step Functions       |
| **Automation**  | GitHub Actions            |
| **Utility Jobs**| AWS Lambda                |

---

## Data Schemas

### Product Data

| Field          | Type    |
|----------------|---------|
| `product_id`   | STRING  |
| `department_id`| STRING  |
| `department`   | STRING  |
| `product_name` | STRING  |

### Orders

| Field            | Type      |
|------------------|-----------|
| `order_num`      | STRING    |
| `order_id`       | STRING    |
| `user_id`        | STRING    |
| `order_timestamp`| TIMESTAMP |
| `total_amount`   | DOUBLE    |
| `date`           | DATE      |

### Order Items

| Field                  | Type      | 
|------------------------|-----------|
| `id`                   | STRING    |
| `order_id`             | STRING    |
| `user_id`              | STRING    |
| `days_since_prior_order`| INTEGER |
| `product_id`           | STRING    |
| `add_to_cart_order`    | INTEGER   |
| `reordered`            | BOOLEAN   |
| `order_timestamp`      | TIMESTAMP |
| `date`                 | DATE      |

---

## ETL Pipeline Stages

### 1. **Trigger (Lambda)**
- Watches for `.csv` files in `s3://lakehouse-e-commerce/raw-data/`
- Triggers the Step Function state machine on upload event

### 2. **Processing (AWS Glue Job)**
- Reads raw CSV files using Apache Spark
- Performs:
  - Type casting
  - Data cleaning
  - Validation
  - Deduplication via `MERGE`
- Writes Delta-formatted, partitioned tables to `s3://lakehouse-e-commerce/lakehouse-dwh/`

### 3. **Archiving (AWS Lambda)**
- Triggered upon Glue job success
- Moves raw files from `raw-data/` to `archive/`

### 4. **Metadata Update (Glue Crawler)**
- Scans Processed Zone
- Updates Glue Data Catalog with Delta table schemas

### 5. **Validation (Amazon Athena)**
- SQL queries check:
  - Primary/foreign key integrity
  - Nullability constraints
  - Expected record counts
- Tables validated:
  - `orders`
  - `order_items`
  - `products`

---

## CI/CD Pipeline (GitHub Actions)

### Trigger:
- Push or Pull Request to `main` branch.

### Workflow Steps:
1. Setup Python environment
2. Run Pytest-based unit tests on Glue ETL scripts
3. Lint and check code quality using `flake8`, `black`
4. Upload Spark scripts to S3
5. Deploy Glue ETL scripts
---

## Deployment Checklist

- Configure S3 bucket structure: `raw/`, `lakehouse-dwh/`, `archive/`
- Deploy Glue Jobs and Crawlers
- Setup Lambda function for `.csv` file triggers
- Deploy Step Function
- Register Athena tables through Glue Crawler
- Validate data in Athena
- Enable GitHub Actions workflows
