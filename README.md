# nyctaxi_databricks_pipeline
# 🚕 NYC Taxi Data Pipeline | Azure Databricks & PySpark

**End-to-End Data Engineering Project | Azure Databricks • PySpark • Delta Lake • Medallion Architecture**

## 📌 Project Overview

This project demonstrates the development of a cloud-based data engineering pipeline using Azure Databricks and Apache Spark.

The pipeline is designed to process New York City Yellow Taxi trip records, transforming raw transportation data into structured datasets suitable for analysis and business reporting.

The project follows the **Medallion Architecture (Bronze, Silver, Gold)** to separate data ingestion, cleaning, transformation, and aggregation.

The main focus is on practical data engineering concepts, including scalable data processing, data quality, Delta Lake tables, and workflow orchestration.

## 🎯 Project Goals

The primary goal was to gain practical experience building an end-to-end data pipeline using technologies commonly found in modern cloud data platforms.

Key objectives:

- Ingest real-world transportation datasets into a cloud environment.
- Organize raw, cleaned, and aggregated data using Medallion Architecture.
- Process large datasets using PySpark DataFrames.
- Work with Parquet files and Delta Lake tables.
- Apply data cleaning, validation, and transformation logic.
- Prepare business-oriented datasets using Spark SQL and PySpark.
- Explore automated workflows and pipeline orchestration in Azure Databricks.

## 📊 Dataset Overview

The project uses the publicly available **NYC Taxi & Limousine Commission (TLC) Trip Record Data**.

The dataset contains information about taxi trips in New York City, including:

| Column | Description |
|---|---|
| `VendorID` | Taxi technology provider identifier |
| `tpep_pickup_datetime` | Trip pickup date and time |
| `tpep_dropoff_datetime` | Trip drop-off date and time |
| `passenger_count` | Number of passengers |
| `trip_distance` | Distance traveled in miles |
| `PULocationID` | Pickup location identifier |
| `DOLocationID` | Drop-off location identifier |
| `fare_amount` | Fare amount before additional charges |
| `tip_amount` | Tip paid |
| `total_amount` | Total amount charged |

The source data is distributed in Parquet format, which supports efficient storage and analytical processing.

**Data source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## 🏗️ Data Architecture

The project uses a three-layer Medallion Architecture.

```text
NYC TLC Trip Data (Parquet)
             |
             v
   Databricks Volume
     00 - Landing
             |
             v
    BRONZE LAYER
    Raw Delta Tables
             |
             v
    SILVER LAYER
   Data Cleaning
   Data Validation
   Transformations
             |
             v
     GOLD LAYER
    Aggregations
  Business Metrics
             |
             v
   Analytics-Ready Data
```

Each layer has a specific responsibility, allowing data to be processed in organized stages.

### 🥉 Bronze Layer — Raw Data

The Bronze layer is responsible for preserving the ingested taxi trip records.

Main responsibilities:

- Read source Parquet files.
- Load raw records into Databricks.
- Preserve source information for further processing.
- Store records in Delta Lake tables.

**Example table:** `nyctaxi.01_bronze.yellow_trips_raw`

### 🥈 Silver Layer — Cleaned Data

The Silver layer prepares the raw records for analytical use.

Transformation activities include:

- Handling missing and null values.
- Validating trip timestamps and numerical fields.
- Filtering invalid or inconsistent records.
- Standardizing column types.
- Creating additional attributes for downstream processing.

**Target tables:**
- `nyctaxi.02_silver.yellow_trips_cleansed`
- `nyctaxi.02_silver.yellow_trips_enriched`

The purpose of this layer is to produce consistent, reliable datasets suitable for further transformations.

### 🥇 Gold Layer — Business Analytics

The Gold layer focuses on preparing data for analytical queries and reporting.

Possible business metrics include:

- Daily number of taxi trips.
- Total revenue by day.
- Average trip distance.
- Average fare amount.
- Trip volume trends over time.

**Target table:** `nyctaxi.03_gold.daily_trip_summary`

This layer demonstrates how raw transportation records can be transformed into meaningful business information.

## 🛠️ Technology Stack

| Technology | Purpose |
|---|---|
| Azure Databricks | Cloud-based data engineering environment |
| Apache Spark | Distributed data processing engine |
| PySpark | Data transformation and processing |
| Spark SQL | SQL-based transformations and aggregations |
| Delta Lake | Reliable analytical table storage |
| Parquet | Efficient source data format |
| Unity Catalog | Data organization and governance |
| Databricks Workflows | Pipeline orchestration |
| Python | Data ingestion and processing logic |
| Git & GitHub | Version control and project documentation |

## ⚙️ Data Engineering Workflow

The project follows a structured data processing approach.

**Step 1 — Data Ingestion**

Download NYC Yellow Taxi trip records and store the source Parquet files in a Databricks Volume.

**Step 2 — Bronze Processing**

Load the files using Apache Spark and prepare raw Delta tables for downstream transformations.

**Step 3 — Silver Processing**

Apply data quality rules and transformations to produce cleaned and enriched trip records.

**Step 4 — Gold Processing**

Aggregate processed records to create datasets designed for business reporting.

**Step 5 — Workflow Orchestration**

Use Databricks workflow functionality to coordinate processing steps and manage execution dependencies.

## 📁 Repository Structure

```text
nyctaxi_databricks_pipeline/
│
├── ad_hoc/
│   └── Exploratory and ad-hoc development
│
├── one_off/
│   └── One-time processing utilities
│
├── transformations/
│   └── notebooks/
│       └── Data transformation notebooks
│
└── README.md
```

The repository separates exploratory activities, one-time processing, and transformation logic.

## 📈 Analytical Use Cases

The processed NYC taxi dataset can support transportation analytics, including:

**Trip Demand Analysis**

Analyze how trip volumes change over time and identify daily or seasonal demand patterns.

**Revenue Analysis**

Calculate total fares and revenues to understand transportation activity from a financial perspective.

**Trip Distance Analysis**

Explore average trip distances and identify differences between short and long trips.

**Operational Trends**

Aggregate large volumes of historical taxi records into smaller, reporting-friendly datasets.

These examples illustrate how the Gold layer can support downstream analytical applications.

## 🧠 Skills Demonstrated

Through this project, I have been practicing:

- End-to-end ETL/ELT pipeline development.
- Cloud data engineering fundamentals.
- Distributed data processing with Apache Spark.
- DataFrame transformations using PySpark.
- Data modeling with Medallion Architecture.
- Working with Delta Lake and Parquet.
- Data validation and quality checks.
- SQL-based analytical aggregation.
- Databricks workspace organization.
- Workflow dependencies and orchestration concepts.
- Git-based source control.

## 🚀 Future Improvements

Potential enhancements include:

- Implementing incremental data ingestion.
- Adding automated data quality tests.
- Introducing workflow monitoring and alerting.
- Improving error handling and logging.
- Adding a Power BI dashboard.
- Implementing CI/CD for Databricks deployments.
- Optimizing Delta tables and Spark transformations.

## 👨‍💻 About the Author

**Yehor Zinchenko**

Aspiring Data Engineer focused on building practical skills in Azure Databricks, PySpark, SQL, and modern data engineering technologies.

This project is part of my hands-on learning journey toward developing scalable and reliable data pipelines.

**GitHub:** [github.com/YehorZinchenko](https://github.com/YehorZinchenko)

---

*Built as a hands-on Data Engineering portfolio project using NYC TLC public transportation data.*
