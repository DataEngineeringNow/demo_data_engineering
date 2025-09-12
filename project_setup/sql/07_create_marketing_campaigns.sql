CREATE TABLE marketing_campaigns (
    campaign_id     BIGINT PRIMARY KEY,
    campaign_name   VARCHAR(100),
    start_date      DATE,
    end_date        DATE,
    channel         VARCHAR(50) -- Email, Google Ads, Facebook, etc.
);
