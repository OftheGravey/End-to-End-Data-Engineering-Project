CREATE TABLE IF NOT EXISTS f_order_events (
    order_event_sk INT,
    order_id INT,
    customer_id INT,
    -- measurements
    op INT,
    ts_ms BIGINT,
    status TEXT NOT NULL,
    shipping_method TEXT,
    order_date TIMESTAMP
);