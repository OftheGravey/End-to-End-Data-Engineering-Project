SELECT
    dor.order_sk,
    dcu.customer_sk,
    dbk.book_sk,
    ddd.date_sk,
    src.quantity,
    src.price_at_purchase,
    src.discount,
    gen_random_uuid() AS order_item_event_sk,
    to_timestamp(src.ts_ms / 1000) AS transaction_time,
    src.quantity * src.price_at_purchase * (1 - src.discount) AS price_total
FROM
    {{ source('staging_db','order_items').identifier }} AS src
INNER JOIN
    {{ source('staging_db','orders').identifier }} AS src_ord
    ON src.order_id = src_ord.order_id
INNER JOIN {{ ref('d_orders').identifier }} AS dor
    ON
        src.order_id = dor.order_id
        AND to_timestamp(src.ts_ms / 1000) >= dor.valid_from
        AND to_timestamp(src.ts_ms / 1000) < dor.valid_to
INNER JOIN {{ ref('d_customers').identifier }} AS dcu
    ON
        src_ord.customer_id = dcu.customer_id
        AND to_timestamp(src.ts_ms / 1000) >= dcu.valid_from
        AND to_timestamp(src.ts_ms / 1000) < dcu.valid_to
INNER JOIN {{ ref('d_books').identifier }} AS dbk ON src.book_id = dbk.book_nk
INNER JOIN
    {{ ref('d_date').identifier }} AS ddd
    ON ddd.date = cast(to_timestamp(src.ts_ms / 1000) AS DATE)
