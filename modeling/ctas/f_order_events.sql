CREATE TABLE IF NOT EXISTS
    f_order_events AS (
        SELECT
            get_random_uuid () AS order_event_sk,
            order_id,
            customer_id,
            op,
            ts_ms,
            status NOT NULL,
            shipping_method,
            order_date
        FROM
            orders
    );