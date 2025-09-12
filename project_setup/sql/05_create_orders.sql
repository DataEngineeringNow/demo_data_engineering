CREATE TABLE orders (
    order_id        BIGINT PRIMARY KEY,
    customer_id     BIGINT,
    seller_id       BIGINT,
    order_date      DATE,
    delivery_date   DATE,
    status          VARCHAR(20), -- delivered, returned, canceled
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);
