DECLARE backfill_flag BOOL DEFAULT TRUE; 

DECLARE dates_to_process ARRAY<DATE>;
DECLARE hours_to_process ARRAY<STRING>;

-- Conditionally populate the dates_to_process array
IF backfill_flag THEN
  SET dates_to_process = (
    SELECT ARRAY_AGG(DISTINCT date ORDER BY date)
    FROM `bigquery-dataflow-460522.retail_ds.inventory_data_ext`
    WHERE date IS NOT NULL 
  );
ELSE
  SET dates_to_process = (
    SELECT [MAX(date)]
    FROM `bigquery-dataflow-460522.retail_ds.inventory_data_ext`
    WHERE date IS NOT NULL
  );
END IF;
-- Conditionally populate the hours_to_process array
IF backfill_flag THEN
  SET hours_to_process = (
      SELECT ARRAY_AGG(DISTINCT hour ORDER BY hour)
      FROM `bigquery-dataflow-460522.retail_ds.inventory_data_ext`
      WHERE hour IS NOT NULL 
    );
  ELSE
    SET hours_to_process = (
      SELECT [MAX(hour)]
      FROM `bigquery-dataflow-460522.retail_ds.inventory_data_ext`
      WHERE date = (SELECT MAX(date) FROM `bigquery-dataflow-460522.retail_ds.inventory_data_ext`)
    );
  END IF;
--SELECT DISTINCT date FROM `bigquery-dataflow-460522.retail_ds.inventory_data_ext` LIMIT 10
CREATE SCHEMA IF NOT EXISTS `bigquery-dataflow-460522.retail_ds`;

CREATE OR REPLACE TABLE `bigquery-dataflow-460522.retail_ds.inventory_data_managed`(

  inventory_date STRING,
  inventory_hour STRING,
  product_id STRING,
  in_stock INT64,
  new_product INT64,
  returned_product INT64,
  units_sold INT64,
  final_stock INT64,
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
FROM `bigquery-dataflow-460522.retail_ds.inventory_data_managed`
WHERE CAST(date AS DATE) IN UNNEST(dates_to_process) AND hour IN UNNEST(hours_to_process);

INSERT INTO `bigquery-dataflow-460522.retail_ds.inventory_data_managed`
SELECT
    ext.inventory_date,
    ext.inventory_hour,
    ext.product_id,
    ext.in_stock,
    ext.new_product,
    ext.returned_product,
    ext.units_sold,
    ext.final_stock,
    CAST(date AS STRING),
    hour,
    TIMESTAMP(date)
FROM `bigquery-dataflow-460522.retail_ds.inventory_data_ext` AS ext
WHERE 
  ext.date IN UNNEST(dates_to_process) 
  AND ext.hour IN UNNEST(hours_to_process)