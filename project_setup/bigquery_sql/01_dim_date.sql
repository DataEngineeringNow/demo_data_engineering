CREATE TABLE ecommerce_dw.dim_date (
  date_id DATE NOT NULL,
  year INT64,
  quarter INT64,
  month INT64,
  week INT64,
  day INT64,
  weekday STRING,
  is_weekend BOOL
);
