CREATE TABLE products (
    product_id      BIGINT PRIMARY KEY,
    product_name    VARCHAR(200),
    category        VARCHAR(50),
    subcategory     VARCHAR(50),
    brand           VARCHAR(50),
    unit_price      DECIMAL(10,2),
    cost_price      DECIMAL(10,2)
);
