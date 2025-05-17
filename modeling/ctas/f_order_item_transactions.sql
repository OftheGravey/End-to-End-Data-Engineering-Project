CREATE TABLE
    IF NOT EXISTS f_order_item_transactions AS (
        SELECT
            gen_random_uuid () AS order_item_event_sk,
            dor.order_sk,
            dcu.customer_id,
            dbk.book_sk,
            date_sk,
            -- measurements
            TO_TIMESTAMP (src.ts_ms / 1000) transaction_time,
            src.quantity,
            src.price_at_purchase,
            src.discount
        FROM
            order_items src
            INNER JOIN orders src_ord ON src_ord.order_id = src.order_id
            INNER JOIN d_orders dor ON src.order_id = dor.order_id
            AND TO_TIMESTAMP (src.ts_ms / 1000) >= dor.valid_from
            AND TO_TIMESTAMP (src.ts_ms / 1000) < dor.valid_to
            INNER JOIN d_customers dcu ON src_ord.customer_id = dcu.customer_id
            AND TO_TIMESTAMP (src.ts_ms / 1000) >= dcu.valid_from
            AND TO_TIMESTAMP (src.ts_ms / 1000) < dcu.valid_to
            INNER JOIN d_books dbk ON src.book_id = dbk.book_nk
            INNER JOIN d_date ddd ON ddd.date = CAST(src.ts_ms AS DATE)
    );