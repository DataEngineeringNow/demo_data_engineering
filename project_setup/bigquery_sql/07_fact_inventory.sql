CREATE TABLE ecommerce_dw.fact_inventory (
  inventory_id STRING NOT NULL,
  product_id STRING,
  seller_id STRING,
  date_id DATE,
  opening_stock INT64,
  stock_in INT64,
  stock_sold INT64,
  closing_stock INT64,
  stockout_flag BOOL,
  restock_date DATE
);
