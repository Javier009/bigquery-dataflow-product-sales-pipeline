# Real-Time Retail Analytics Pipeline on Google Cloud Platform

This project demonstrates an end-to-end data pipeline built on Google Cloud Platform (GCP) for generating, ingesting, processing, storing, and analyzing retail data (product catalog, sales, and inventory). The pipeline simulates a real-world scenario where data is generated hourly, stored in a data lake, streamed for real-time alerts, and loaded into BigQuery for batch analytics.

## Table of Contents

- [Real-Time Retail Analytics Pipeline on Google Cloud Platform](#real-time-retail-analytics-pipeline-on-google-cloud-platform)
  - [Table of Contents](#table-of-contents)
  - [Project Overview](#project-overview)
  - [Architecture Diagram (Conceptual)](#architecture-diagram-conceptual)
  - [Core Components and Workflow](#core-components-and-workflow)
    - [1. Data Generation and Cloud Storage](#1-data-generation-and-cloud-storage)
    - [2. Real-time Inventory Alerting (Pub/Sub \& Dataflow)](#2-real-time-inventory-alerting-pubsub--dataflow)
    - [3. BigQuery Data Warehousing](#3-bigquery-data-warehousing)
      - [External Tables with Hive Partitioning](#external-tables-with-hive-partitioning)
      - [Managed Tables with Incremental Updates](#managed-tables-with-incremental-updates)
    - [4. Analytics and Reporting](#4-analytics-and-reporting)
    - [5. Automation](#5-automation)
  - [Detailed Implementation Steps](#detailed-implementation-steps)
    - [Prerequisites](#prerequisites)
    - [Setup Instructions](#setup-instructions)
  - [Key Learnings and Potential Enhancements](#key-learnings-and-potential-enhancements)

---

## Project Overview

The primary goal of this project is to build a scalable and robust pipeline that handles various aspects of retail data management:

* **Simulated Data Generation:** Creating realistic product, sales, and inventory data streams.
* **Data Lake Storage:** Utilizing Google Cloud Storage (GCS) with Hive partitioning for efficient raw data storage.
* **Real-time Streaming:** Implementing alerts for critical events like negative inventory using Pub/Sub and Dataflow.
* **Data Warehousing:** Leveraging BigQuery for both staging (external tables) and optimized analytics (managed tables).
* **Automation:** Scheduling data generation and BigQuery operations for continuous processing.

---

## Architecture Diagram (Conceptual)

[Cloud Scheduler (Hourly)] ----triggers----> [Cloud Run/Function (Python Script)]
│
├─ Generates: Product Catalog, Sales, Inventory
│
├─ Uploads CSVs to GCS (Hive Partitioned: /date=.../hour=...)
│   ├── gs://&lt;bucket>/product_catalog/date=.../hour=.../
│   ├── gs://&lt;bucket>/sales_data/date=.../hour=.../
│   └── gs://&lt;bucket>/inventory_data/date=.../hour=.../
│
└─ IF (inventory &lt; 0) THEN Publish to [Pub/Sub Topic: inventory-updates]
│
└──triggers──> [Dataflow Streaming Job (Pub/Sub to BigQuery Template)]
│
└───writes───> [BigQuery Table: negative_inventory_alerts]

[BigQuery]
├─ External Table (product_catalog_ext) ──reads──> GCS Product Catalog Data
├─ External Table (sales_data_ext) ───────reads──> GCS Sales Data
├─ External Table (inventory_data_ext) ───reads──> GCS Inventory Data
│
└─ [Cloud Scheduler (Hourly)] --triggers--> [BigQuery Scheduled Queries/Pipeline]
│
├─ Query 1: INSERT/MERGE into Managed Table (product_catalog_managed) FROM External Table (product_catalog_ext) - Incremental
├─ Query 2: INSERT/MERGE into Managed Table (sales_data_managed) FROM External Table (sales_data_ext) - Incremental
├─ Query 3: INSERT/MERGE into Managed Table (inventory_data_managed) FROM External Table (inventory_data_ext) - Incremental
└─ Query 4: Analytical Query (e.g., Sales by Category) on Managed Tables


---

## Core Components and Workflow

### 1. Data Generation and Cloud Storage

* **Python Script:** A core Python script is responsible for generating synthetic retail data.
    * **Product Catalog:** Creates a base catalog of products with details like ID, name, category, brand, price, etc.
    * **Pre-Sales Inventory:** Initializes an inventory state based on the product catalog for a given period (date/hour). This acts as the opening stock.
    * **Sales Data:** Simulates sales transactions for products from the catalog, including quantity sold.
    * **Inventory Updates:** Calculates the final inventory based on the pre-sales inventory, new product arrivals (if any, assumed in pre-sales for this version), returns (if any, assumed in pre-sales), and units sold. The script ensures all data generated is logically linked, primarily originating from the product catalog.
* **Google Cloud Storage (GCS):**
    * The generated Product Catalog, Sales Data, and final Inventory Updates are stored as CSV files in a GCS bucket.
    * **Hive Partitioning:** Data is stored using a Hive-compatible directory structure:
        * `gs://<YOUR_BUCKET>/product_catalog/date=YYYY-MM-DD/hour=HH/file.csv`
        * `gs://<YOUR_BUCKET>/sales_data/date=YYYY-MM-DD/hour=HH/file.csv`
        * `gs://<YOUR_BUCKET>/inventory_data/date=YYYY-MM-DD/hour=HH/file.csv`
    * This partitioning by `date` and `hour` allows for efficient querying and data management in BigQuery external tables.
    * The script is designed to process data for a rolling window (e.g., the last N days up to the previous complete hour), creating new files only if they don't already exist for a specific date/hour.

### 2. Real-time Inventory Alerting (Pub/Sub & Dataflow)

* **Critical Event Detection:** During the inventory update process within the Python script, if a product's calculated `final_stock` becomes negative, this critical event is identified.
* **Google Cloud Pub/Sub:**
    * A Pub/Sub topic (e.g., `inventory-updates-streaming`) is used to publish messages for each negative inventory event.
    * Each message is a JSON payload containing details of the product with negative stock (e.g., inventory date, hour, product ID, stock levels).
* **Google Cloud Dataflow:**
    * A **streaming Dataflow job** subscribes to the Pub/Sub topic.
    * This job utilizes an existing **Google-provided template** (e.g., "Cloud Pub/Sub Topic to BigQuery") to read messages from the topic and write them directly into a dedicated BigQuery table (e.g., `negative_inventory_alerts`). This provides near real-time visibility into stock issues. Using a template simplifies development by avoiding the need to write custom Apache Beam code for this specific task.

### 3. BigQuery Data Warehousing

#### External Tables with Hive Partitioning

* Three **external BigQuery tables** are created, mapping to the Hive-partitioned data in GCS:
    * `retail_ds.product_catalog_ext`
    * `retail_ds.sales_data_ext`
    * `retail_ds.inventory_data_ext`
* These tables use `WITH PARTITION COLUMNS (date DATE, hour STRING)` to leverage the `date=` and `hour=` directory structure in GCS.
* **Automatic Updates:** As new data files are added to the GCS directories in the correct Hive partition format (by the Python script), these external tables automatically reflect the new data without requiring explicit `LOAD` or `INSERT` statements for the external tables themselves.

#### Managed Tables with Incremental Updates

* For enhanced query performance, data governance, and to have data stored within BigQuery's optimized storage, **managed (native) BigQuery tables** are created:
    * `retail_ds.product_catalog_managed`
    * `retail_ds.sales_data_managed`
    * `retail_ds.inventory_data_managed`
* These managed tables are also partitioned (e.g., by an `ingestion_date` or `event_date` column derived from the data).
* **Incremental Loading Logic:** Instead of a simple `SELECT * FROM external_table`, specific SQL logic (e.g., using `INSERT OVERWRITE PARTITIONS` or `MERGE`) is implemented to populate these managed tables. This logic focuses on:
    * Identifying the latest data available in the external tables (e.g., based on the `date` and `hour` partition columns from the external table).
    * Inserting or merging only this new/updated data into the managed tables, effectively updating the managed tables with the latest values from GCS. This avoids full reloads and ensures the managed tables are incrementally built.

### 4. Analytics and Reporting

* **Joining Managed Tables:** A sample analytical query is provided that joins the `product_catalog_managed` and `sales_data_managed` tables.
* **Business Insights:** This query calculates total sales quantity and revenue, aggregated by product category for specific dates, using `date` (derived from the partition or an internal date column) and `product_id` as join keys. This demonstrates how the curated managed tables can be used for business intelligence and reporting.

### 5. Automation

* **Data Generation and GCS Upload:**
    * The Python script responsible for data generation and GCS upload is deployed (e.g., as a Cloud Run service or Cloud Function).
    * A **Cloud Scheduler job** is configured to trigger this script **every hour**, ensuring a continuous flow of new hourly data into GCS.
* **BigQuery Operations (Managed Table Updates & Analytics):**
    * The SQL statements for updating the managed BigQuery tables from the external tables, and any subsequent analytical queries, are organized into a **BigQuery Scheduled Query Pipeline**.
    * This pipeline defines the sequence of SQL tasks (e.g., update catalog_managed, then update sales_managed, then run aggregation query).
    * Another **Cloud Scheduler job** triggers this BigQuery pipeline **every hour** (typically scheduled to run after the data generation job has completed).

---

## Detailed Implementation Steps

*(This section would typically include code snippets, gcloud commands, and UI steps. For this README, I'll outline what would go here.)*

### Prerequisites

* Google Cloud Platform Account with billing enabled.
* `gcloud` CLI installed and configured.
* Python 3.x installed.
* Required Python libraries (listed in `requirements.txt`, e.g., `google-cloud-storage`, `google-cloud-pubsub`, `Faker`, `pytz`, `Flask`).
* Appropriate IAM permissions for creating and managing GCS, Pub/Sub, Dataflow, BigQuery, and Cloud Scheduler resources.

### Setup Instructions

1.  **Project Setup:**
    * Create a new GCP project or select an existing one.
    * Enable necessary APIs: Cloud Storage, Pub/Sub, Dataflow, BigQuery, Cloud Scheduler, Cloud Run (or Cloud Functions), Cloud Build (for deploying services).
2.  **GCS Bucket Creation:**
    * Create a GCS bucket (e.g., `retail_data_v1`) with the desired storage class and location.
3.  **Pub/Sub Topic Creation:**
    * Create the Pub/Sub topic `inventory-updates-streaming`.
4.  **Python Script Deployment (Data Generation):**
    * Provide the Python script (`main.py` containing `generate_data` and helper functions).
    * Instructions for deploying it as a Cloud Run service (including `Dockerfile` and `cloudbuild.yaml` if applicable) or a Cloud Function.
    * Set up IAM permissions for the Cloud Run/Function service account to write to GCS and Pub/Sub.
5.  **Dataflow Job Creation:**
    * Instructions for launching the "Cloud Pub/Sub Topic to BigQuery" Dataflow template:
        * Specify the input Pub/Sub topic (`inventory-updates-streaming`).
        * Specify the output BigQuery table (e.g., `your_project.retail_ds.negative_inventory_alerts`) and schema for this table.
        * Set necessary job parameters.
6.  **BigQuery Setup:**
    * Provide the SQL DDL script to:
        * `CREATE SCHEMA IF NOT EXISTS retail_ds;`
        * `CREATE EXTERNAL TABLE ... product_catalog_ext ...` (with Hive partitioning).
        * `CREATE EXTERNAL TABLE ... sales_data_ext ...` (with Hive partitioning).
        * `CREATE EXTERNAL TABLE ... inventory_data_ext ...` (with Hive partitioning).
        * `CREATE TABLE ... product_catalog_managed ...` (partitioned and clustered native table).
        * `CREATE TABLE ... sales_data_managed ...` (partitioned and clustered native table).
        * `CREATE TABLE ... inventory_data_managed ...` (partitioned and clustered native table).
7.  **Automation Setup (Cloud Scheduler):**
    * **Job 1 (Data Generation):**
        * Target: HTTP endpoint of the deployed Cloud Run/Function.
        * Frequency: Hourly (e.g., `0 * * * *`).
        * Payload (if needed).
    * **Job 2 (BigQuery Pipeline):**
        * Target: BigQuery "Run scheduled query" or trigger for the saved BigQuery pipeline.
        * Frequency: Hourly (e.g., `15 * * * *` - scheduled after data generation).
        * Provide the SQL for the BigQuery pipeline tasks (loading managed tables, running analytical queries).

---

## Key Learnings and Potential Enhancements

* **Learnings:**
    * Effective use of GCS Hive partitioning for organizing data lake storage and enabling efficient external table queries.
    * Combining batch (BigQuery managed table loads) and stream processing (Pub/Sub to Dataflow for alerts).
    * Leveraging GCP-managed services (Dataflow templates, Cloud Scheduler, BigQuery external tables) to reduce operational overhead.
    * Importance of incremental loading strategies for managed BigQuery tables.
    * Orchestration using Cloud Scheduler for both data generation and BigQuery processing.
* **Potential Enhancements:**
    * **Error Handling and Monitoring:** Implement more robust error handling in the Python script and Dataflow job. Set up Cloud Monitoring alerts for pipeline failures.
    * **Dead Letter Queues (DLQ):** For Pub/Sub messages that cannot be processed by Dataflow.
    * **Data Validation:** Add data validation steps (e.g., using BigQuery or Dataflow) to check the quality of incoming data before loading into curated managed tables.
    * **More Complex Transformations:** Use Dataflow for more complex transformations if SQL capabilities in BigQuery are insufficient.
    * **Advanced Orchestration:** For more complex dependencies and workflow management, consider using Cloud Composer (Apache Airflow).
    * **Security:** Implement fine-grained IAM controls, VPC Service Controls, and data encryption best practices.
    * **Cost Optimization:** Monitor BigQuery slot usage and GCS storage costs; optimize queries and data lifecycle management.
    * **Idempotency:** Ensure data generation and loading steps are idempotent, especially if retries occur.
    * **CI/CD:** Set up a CI/CD pipeline for deploying the Python script and any infrastructure changes.

---

This `README.md` provides a comprehensive overview. You would fill in the "Setup Instructions" with your specific code snippets, `gcloud` commands, and detailed steps. Remember to replace placeholders like `<YOUR_BUCKET>` with actual values.