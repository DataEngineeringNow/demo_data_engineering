CREATE TABLE `ecommerce_dw.fact_sales`
(
  sales_id STRING,
  order_id STRING,
  customer_id STRING,
  product_id STRING,
  seller_id STRING,
  date_id DATETIME,
  quantity INT64,
  gross_value FLOAT64,
  discount FLOAT64,
  tax FLOAT64,
  net_revenue FLOAT64,
  cost FLOAT64,
  profit FLOAT64,
  order_date DATETIME,
  delivery_date DATETIME,
  fulfillment_time INT64
);