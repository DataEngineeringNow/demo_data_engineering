CREATE TABLE ecommerce_dw.fact_cart (
  cart_event_id STRING NOT NULL,
  customer_id STRING,
  product_id STRING,
  date_id DATE,
  event_type STRING,
  quantity INT64,
  cart_session_id STRING
);
