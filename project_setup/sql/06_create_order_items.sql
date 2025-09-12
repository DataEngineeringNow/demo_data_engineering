CREATE TABLE order_items (
    order_item_id   BIGSERIAL PRIMARY KEY,
    order_id        BIGINT,
    product_id      BIGINT,
    quantity        INT,
    unit_price      DECIMAL(10,2),
    discount        DECIMAL(10,2),
    tax             DECIMAL(10,2),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
