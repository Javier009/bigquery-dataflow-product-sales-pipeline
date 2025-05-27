DECLARE backfill_flag BOOL DEFAULT TRUE; 

DECLARE dates_to_process ARRAY<DATE>;
DECLARE hours_to_process ARRAY<STRING>;

-- Conditionally populate the dates_to_process array
IF backfill_flag THEN
  SET dates_to_process = (
    SELECT ARRAY_AGG(DISTINCT date ORDER BY date)
    FROM `bigquery-dataflow-460522.retail_ds.sales_data_ext`
    WHERE date IS NOT NULL 
  );
ELSE
  SET dates_to_process = (
    SELECT [MAX(date)]
    FROM `bigquery-dataflow-460522.retail_ds.sales_data_ext`
    WHERE date IS NOT NULL
  );
END IF;
-- Conditionally populate the hours_to_process array
IF backfill_flag THEN
  SET hours_to_process = (
      SELECT ARRAY_AGG(DISTINCT hour ORDER BY hour)
      FROM `bigquery-dataflow-460522.retail_ds.sales_data_ext`
      WHERE hour IS NOT NULL 
    );
  ELSE
    SET hours_to_process = (
      SELECT [MAX(hour)]
      FROM `bigquery-dataflow-460522.retail_ds.sales_data_ext`
      WHERE date = (SELECT MAX(date) FROM `bigquery-dataflow-460522.retail_ds.sales_data_ext`)
    );
  END IF;

CREATE SCHEMA IF NOT EXISTS `bigquery-dataflow-460522.retail_ds`;

CREATE OR REPLACE TABLE `bigquery-dataflow-460522.retail_ds.sales_data_managed` (

  transaction_date STRING,
  transaction_hour STRING,
  transaction_id STRING,
  customer_id STRING,
  order_country STRING,
  product_id STRING,
  unit_price FLOAT64,
  units_sold INT64,
  discount_applied FLOAT64,
  total_ammount_paid FLOAT64,
  date STRING,
  hour STRING,
  date_tmsp TIMESTAMP

)
PARTITION BY DATE(date_tmsp)
CLUSTER BY product_id
OPTIONS (
  description = "Product sales data for retail Data Set table - Managed Table. This comes from Ext. Hive Partitioned table",
  labels = [('env', 'dev'), ('source', 'csv')],
  require_partition_filter = FALSE
);

# Delete rows before processing --> OVERWRITING NEW DATA IN CASE IT EXISTS
DELETE 
FROM `bigquery-dataflow-460522.retail_ds.sales_data_managed`
WHERE CAST(date AS DATE) IN UNNEST(dates_to_process) AND hour IN UNNEST(hours_to_process);

INSERT INTO `bigquery-dataflow-460522.retail_ds.sales_data_managed`
SELECT
    ext.transaction_date,
    ext.transaction_hour,
    ext.transaction_id,
    ext.customer_id,        
    ext.order_country,       
    ext.product_id,
    ext.unit_price,
    ext.units_sold,
    ext.discount_applied,
    ext.total_ammount_paid,
    CAST(date AS STRING),
    hour,
    TIMESTAMP(date)
FROM `bigquery-dataflow-460522.retail_ds.sales_data_ext` AS ext
WHERE 
  ext.date IN UNNEST(dates_to_process) 
  AND ext.hour IN UNNEST(hours_to_process)