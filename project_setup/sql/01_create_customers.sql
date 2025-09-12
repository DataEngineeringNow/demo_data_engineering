CREATE TABLE customers (
    customer_id     BIGINT PRIMARY KEY,
    customer_name   VARCHAR(100),
    email           VARCHAR(100),
    phone           VARCHAR(20),
    location        VARCHAR(100),
    signup_date     DATE,
    acquisition_channel VARCHAR(50), -- Organic, Ads, Referral, Social, etc.
    acquisition_cost DECIMAL(10,2)
);
