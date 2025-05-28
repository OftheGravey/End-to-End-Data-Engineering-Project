-- Test group for Flink Tables

CREATE TABLE authors (
    author_id INTEGER NOT NULL,
    first_name STRING NOT NULL,
    last_name STRING NOT NULL,
    biography STRING,
    country STRING,
    origin_ts TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
    event_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
    origin_database STRING METADATA FROM 'value.source.database' VIRTUAL,
    origin_schema STRING METADATA FROM 'value.source.schema' VIRTUAL,
    origin_table STRING METADATA FROM 'value.source.table' VIRTUAL,
    origin_properties MAP<STRING, STRING> METADATA FROM 'value.source.properties' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg-changes.public.authors',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'testGroup',
    'format' = 'debezium-json',
    'scan.startup.mode' = 'latest-offset'
);
CREATE TABLE orders (
    order_id INT,
    customer_id INT,
    order_date BIGINT,
    status STRING,
    shipping_method STRING,
    notes STRING,
    headers STRING METADATA FROM 'headers' VIRTUAL,
    topic STRING METADATA FROM 'topic' VIRTUAL,
    leader_epoch STRING METADATA FROM 'leader-epoch' VIRTUAL,
    origin_ts TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
    event_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
    origin_database STRING METADATA FROM 'value.source.database' VIRTUAL,
    origin_schema STRING METADATA FROM 'value.source.schema' VIRTUAL,
    origin_table STRING METADATA FROM 'value.source.table' VIRTUAL,
    origin_properties MAP<STRING, STRING> METADATA FROM 'value.source.properties' VIRTUAL
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg-changes.public.orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'testGroup',
    'format' = 'debezium-json',
    'scan.startup.mode' = 'latest-offset',
    'debezium-json.schema-include' = 'true'
);

CREATE TABLE orders (
    raw_json STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'pg-changes.public.orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'testGroup',
    'format' = 'raw',
    'scan.startup.mode' = 'latest-offset'
);

-- SHIPMENT EVENTS
CREATE TABLE IF NOT EXISTS shipments (
    shipment_id INTEGER,
    order_id INTEGER,
    carrier_id INTEGER,
    service_id INTEGER,
    tracking_number VARCHAR,
    shipping_status VARCHAR,
    shipped_date DATE,
    expected_delivery_date DATE,
    actual_delivery_date DATE,
    shipping_cost DECIMAL(10, 2),
    op STRING,
    emitted_ts_ms BIGINT,
    ts_ms BIGINT,
    connector_version STRING,
    transaction_id STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'mysql-changes.shipping_db.shipments',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'testGroup',
    'format' = 'debezium-json',
    'scan.startup.mode' = 'earliest-offset'
);