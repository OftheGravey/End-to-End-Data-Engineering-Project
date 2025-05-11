CREATE TABLE IF NOT EXISTS f_shipment_events (
    shipment_event_sk INT,
    shipment_event_id INT,
    -- measurements
    status VARCHAR,
    location VARCHAR,
    event_time TIMESTAMP,
    notes TEXT,
    op INT,
    ts_ms BIGINT,
    status TEXT NOT NULL,
    shipping_method TEXT
);