CREATE TABLE IF NOT EXISTS r_customer_events (
    customer_event_sk INTEGER,
    customer_id INTEGER,
    op VARCHAR,
    ts_ms BIGINT,
    -- mutable attributes for customers
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT NOT NULL,
    phone TEXT,
    created_at TIMESTAMP,
    street_address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    country TEXT,
)