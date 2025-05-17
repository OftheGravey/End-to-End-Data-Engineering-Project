SELECT
    gen_random_uuid () AS order_item_event_sk,
    dor.order_sk,
    dcu.customer_sk,
    dbk.book_sk,
    date_sk,
    -- measurements
    TO_TIMESTAMP (src.ts_ms / 1000) AS transaction_time,
    src.quantity,
    src.price_at_purchase,
    src.discount,
    src.quantity * src.price_at_purchase * (1 - src.discount) AS price_total
FROM
    {{ source('staging_db','order_items').identifier }} src
    INNER JOIN {{ source('staging_db','orders').identifier }} src_ord ON src_ord.order_id = src.order_id
    INNER JOIN {{ ref('d_orders').identifier }} dor ON src.order_id = dor.order_id
    AND TO_TIMESTAMP (src.ts_ms / 1000) >= dor.valid_from
    AND TO_TIMESTAMP (src.ts_ms / 1000) < dor.valid_to
    INNER JOIN {{ ref('d_customers').identifier }} dcu ON src_ord.customer_id = dcu.customer_id
    AND TO_TIMESTAMP (src.ts_ms / 1000) >= dcu.valid_from
    AND TO_TIMESTAMP (src.ts_ms / 1000) < dcu.valid_to
    INNER JOIN {{ ref('d_books').identifier }} dbk ON src.book_id = dbk.book_nk
    INNER JOIN {{ ref('d_date').identifier }} ddd ON ddd.date = CAST(TO_TIMESTAMP (src.ts_ms / 1000) AS DATE)