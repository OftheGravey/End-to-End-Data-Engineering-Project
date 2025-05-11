CREATE TABLE f_order_item_events AS (
    order_item_event_sk INT,
    order_id INT,
    customer_id INT,
    book_id INT,
    -- measurements
    op INT,
    ts_ms BIGINT,
    quantity INT,
    price_at_purchase DOUBLE,
    discount DOUBLE
);