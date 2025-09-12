CREATE TABLE ecommerce_dw.dim_customer (
  customer_id STRING NOT NULL,
  name STRING,
  email STRING,
  phone STRING,
  location STRING,
  acquisition_channel STRING,
  loyalty_segment STRING,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
