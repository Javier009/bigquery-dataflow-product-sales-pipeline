SELECT
  A.date, 
  B.category_name,
SUM(A.units_sold) AS units_sold,
SUM(A.total_ammount_paid) AS total_ammount_paid
FROM `bigquery-dataflow-460522.retail_ds.sales_data_managed` AS A
LEFT JOIN `bigquery-dataflow-460522.retail_ds.product_catalog_managed` AS B
USING (date, product_id)
WHERE B.category_name IS NOT NULL
GROUP BY 1,2
