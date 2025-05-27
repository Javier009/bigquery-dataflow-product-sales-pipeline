CREATE SCHEMA IF NOT EXISTS `bigquery-dataflow-460522.retail_ds`;


#Product Catalog External Table

CREATE EXTERNAL TABLE IF NOT EXISTS `bigquery-dataflow-460522.retail_ds.product_catalog_ext` (

  ingestion_date STRING,
  ingestion_hour STRING,
  product_id STRING,
  product_name STRING,
  category_id STRING,
  category_name STRING,
  brand STRING,
  description STRING,
  unit_price FLOAT64,
  supplier_name STRING,
  tags STRING

)
WITH PARTITION COLUMNS (
  date DATE,
  hour STRING
)
OPTIONS (
  format = 'csv',
  uris = ['gs://retail_data_v1/product_catalog/*.csv'],
  hive_partition_uri_prefix = 'gs://retail_data_v1/product_catalog/',
  skip_leading_rows = 1
);

# Sales Data External Table

CREATE EXTERNAL TABLE IF NOT EXISTS `bigquery-dataflow-460522.retail_ds.sales_data_ext` (

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
)
WITH PARTITION COLUMNS (
  date DATE,
  hour STRING
)
OPTIONS (
  format = 'csv',
  uris = ['gs://retail_data_v1/sales_data/*.csv'],
  hive_partition_uri_prefix = 'gs://retail_data_v1/sales_data/',
  skip_leading_rows = 1
);

# Inventory Updates External Table

CREATE EXTERNAL TABLE IF NOT EXISTS `bigquery-dataflow-460522.retail_ds.inventory_data_ext` (
  inventory_date STRING,
  inventory_hour STRING,
  product_id STRING,
  in_stock INT64,
  new_product INT64,
  returned_product INT64,
  units_sold INT64,
  final_stock INT64
)
WITH PARTITION COLUMNS (
  date DATE,
  hour STRING
)
OPTIONS (
  format = 'csv',
  uris = ['gs://retail_data_v1/inventory_data/*.csv'],
  hive_partition_uri_prefix = 'gs://retail_data_v1/inventory_data/',
  skip_leading_rows = 1
);
