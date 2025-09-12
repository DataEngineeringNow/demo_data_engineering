CREATE TABLE ecommerce_dw.fact_marketing (
  marketing_id STRING NOT NULL,
  campaign_id STRING,
  customer_id STRING,
  date_id DATE,
  impressions INT64,
  clicks INT64,
  conversions INT64,
  spend NUMERIC,
  cpc NUMERIC,
  cpa NUMERIC,
  ctr FLOAT64
);
