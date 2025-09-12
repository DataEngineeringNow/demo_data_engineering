CREATE TABLE campaign_performance (
    campaign_id     BIGINT,
    customer_id     BIGINT,
    date            DATE,
    impressions     INT,
    clicks          INT,
    conversions     INT,
    cost_spent      DECIMAL(12,2),
    PRIMARY KEY (campaign_id, customer_id, date),
    FOREIGN KEY (campaign_id) REFERENCES marketing_campaigns(campaign_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
