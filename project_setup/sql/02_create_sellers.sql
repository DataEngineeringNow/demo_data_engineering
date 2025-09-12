CREATE TABLE sellers (
    seller_id       BIGINT PRIMARY KEY,
    seller_name     VARCHAR(100),
    location        VARCHAR(100),
    rating          DECIMAL(3,2),
    join_date       DATE,
    category_specialization VARCHAR(50)
);
