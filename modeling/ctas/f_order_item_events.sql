CREATE TABLE
    f_order_item_events AS (
        SELECT
            get_random_uuid () AS order_item_event_sk,
            order_id,
            customer_id,
            book_id,
            -- measurements
            op,
            ts_ms,
            quantity,
            price_at_purchase,
            discount
        FROM
            order_items
    );