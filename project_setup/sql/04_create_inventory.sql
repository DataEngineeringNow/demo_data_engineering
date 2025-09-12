CREATE TABLE inventory (
    inventory_id    BIGSERIAL PRIMARY KEY,
    product_id      BIGINT,
    warehouse_id    BIGINT,
    stock_available INT,
    stock_threshold INT,
    restock_date    DATE,
    last_updated    TIMESTAMP,
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
