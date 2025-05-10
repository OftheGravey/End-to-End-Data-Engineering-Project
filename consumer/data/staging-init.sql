-- AUTHORS
CREATE TABLE IF NOT EXISTS authors (
    author_id INTEGER,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    biography TEXT,
    country TEXT,

    -- Debezium CDC metadata
    op TEXT,
    emitted_ts_ms BIGINT,
    ts_ms BIGINT,
    connector_version TEXT,
    transaction_id TEXT,
    lsn BIGINT
);

-- BOOKS
CREATE TABLE IF NOT EXISTS books (
    book_id INTEGER,
    title TEXT NOT NULL,
    author_id INTEGER NOT NULL,
    isbn TEXT NOT NULL,
    price DOUBLE NOT NULL,
    published_date DATE,
    description TEXT,
    genre TEXT,
    stock INTEGER NOT NULL,

    -- Debezium CDC metadata
    op TEXT,
    emitted_ts_ms BIGINT,
    ts_ms BIGINT,
    connector_version TEXT,
    transaction_id TEXT,
    lsn BIGINT
);

-- CUSTOMERS
CREATE TABLE IF NOT EXISTS customers (
    customer_id INTEGER,
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

    -- Debezium CDC metadata
    op TEXT,
    emitted_ts_ms BIGINT,
    ts_ms BIGINT,
    connector_version TEXT,
    transaction_id TEXT,
    lsn BIGINT
);

-- ORDERS
CREATE TABLE IF NOT EXISTS orders (
    order_id INTEGER,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP,
    status TEXT NOT NULL,
    shipping_method TEXT,

    -- Debezium CDC metadata
    op TEXT,
    emitted_ts_ms BIGINT,
    ts_ms BIGINT,
    connector_version TEXT,
    transaction_id TEXT,
    lsn BIGINT
);

-- ORDER_ITEMS
CREATE TABLE IF NOT EXISTS order_items (
    order_item_id INTEGER,
    order_id INTEGER NOT NULL,
    book_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    price_at_purchase DOUBLE NOT NULL,
    discount DOUBLE DEFAULT 0,

    -- Debezium CDC metadata
    op TEXT,
    emitted_ts_ms BIGINT,
    ts_ms BIGINT,
    connector_version TEXT,
    transaction_id TEXT,
    lsn BIGINT
);
