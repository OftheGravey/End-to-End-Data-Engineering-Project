CREATE SCHEMA landing_db;

CREATE SCHEMA staging_db;

CREATE SCHEMA modeling_db;

CREATE DATABASE dagster;

-- store_db
-- AUTHORS
CREATE TABLE
    IF NOT EXISTS landing_db.authors_sink (
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
CREATE TABLE
    IF NOT EXISTS landing_db.books_sink (
        book_id INTEGER,
        title TEXT NOT NULL,
        author_id INTEGER NOT NULL,
        isbn TEXT NOT NULL,
        price DOUBLE PRECISION NOT NULL,
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
CREATE TABLE
    IF NOT EXISTS landing_db.customers_sink (
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
CREATE TABLE
    IF NOT EXISTS landing_db.orders_sink (
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
CREATE TABLE
    IF NOT EXISTS landing_db.order_items_sink (
        order_item_id INTEGER,
        order_id INTEGER NOT NULL,
        book_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        price_at_purchase DOUBLE PRECISION NOT NULL,
        discount DOUBLE PRECISION DEFAULT 0,
        -- Debezium CDC metadata
        op TEXT,
        emitted_ts_ms BIGINT,
        ts_ms BIGINT,
        connector_version TEXT,
        transaction_id TEXT,
        lsn BIGINT
    );

-- shipping_db
-- CARRIERS
CREATE TABLE
    IF NOT EXISTS landing_db.carriers_sink (
        carrier_id INTEGER,
        name VARCHAR,
        contact_email VARCHAR,
        phone VARCHAR,
        tracking_url_template VARCHAR,
        op TEXT,
        emitted_ts_ms BIGINT,
        ts_ms BIGINT,
        connector_version TEXT,
        transaction_id TEXT
    );

-- SHIPPING SERVICES
CREATE TABLE
    IF NOT EXISTS landing_db.shipping_services_sink (
        service_id INTEGER,
        carrier_id INTEGER,
        service_name VARCHAR,
        estimated_days INTEGER,
        cost_estimate NUMERIC(10, 2),
        op TEXT,
        emitted_ts_ms BIGINT,
        ts_ms BIGINT,
        connector_version TEXT,
        transaction_id TEXT
    );

-- SHIPMENTS
CREATE TABLE
    IF NOT EXISTS landing_db.shipments_sink (
        shipment_id INTEGER,
        order_id INTEGER,
        carrier_id INTEGER,
        service_id INTEGER,
        tracking_number VARCHAR,
        shipping_status VARCHAR,
        shipped_date DATE,
        expected_delivery_date DATE,
        actual_delivery_date DATE,
        shipping_cost NUMERIC(10, 2),
        op TEXT,
        emitted_ts_ms BIGINT,
        ts_ms BIGINT,
        connector_version TEXT,
        transaction_id TEXT
    );

-- SHIPMENT EVENTS
CREATE TABLE
    IF NOT EXISTS landing_db.shipment_events_sink (
        event_id INTEGER,
        shipment_id INTEGER,
        status VARCHAR,
        location VARCHAR,
        event_time TIMESTAMP,
        notes TEXT,
        op TEXT,
        emitted_ts_ms BIGINT,
        ts_ms BIGINT,
        connector_version TEXT,
        transaction_id TEXT
    );

-- Dimension Tables:
CREATE TABLE
    IF NOT EXISTS modeling_db.d_orders (
        orderSk VARCHAR PRIMARY KEY,
        orderId INTEGER,
        status VARCHAR,
        shippingMethod VARCHAR,
        orderDate DATE,
        validFrom TIMESTAMP,
        validTo TIMESTAMP
    );

CREATE TABLE
    IF NOT EXISTS modeling_db.d_customers (
        customerSk TEXT PRIMARY KEY,
        customerId INTEGER,
        firstName TEXT NOT NULL,
        lastName TEXT NOT NULL,
        email TEXT NOT NULL,
        phone TEXT,
        streetAddress TEXT,
        city TEXT,
        state TEXT,
        postalCode TEXT,
        country TEXT,
        validFrom TIMESTAMP NOT NULL,
        validTo TIMESTAMP NOT NULL
    );

CREATE TABLE IF NOT EXISTS
    modeling_db.d_books (
        bookSk VARCHAR PRIMARY KEY,
        bookId INTEGER, 
        authorId INTEGER,
        title VARCHAR,
        isbn VARCHAR,
        publishedDate DATE,
        genre VARCHAR,
        authorFirstName VARCHAR,
        authorLastName VARCHAR,
        authorCountry VARCHAR,
        validFrom TIMESTAMP NOT NULL,
        validTo TIMESTAMP NOT NULL
    );