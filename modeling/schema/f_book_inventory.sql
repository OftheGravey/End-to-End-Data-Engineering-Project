CREATE TABLE IF NOT EXISTS f_book_inventory (
    book_stock_event_sk INTEGER,
    book_id INTEGER,
    -- measurements
    stock INTEGER NOT NULL,
    price DOUBLE NOT NULL,
    op TEXT,
    ts_ms BIGINT
);