CREATE TABLE cart_events (
    cart_event_id   BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT,
    product_id      BIGINT,
    event_time      TIMESTAMP,
    event_type      VARCHAR(20), -- add, remove, checkout, purchase
    quantity        INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);
