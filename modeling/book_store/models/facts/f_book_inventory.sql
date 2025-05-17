SELECT
    ddd.date_sk,
    dbk.book_sk,
    MIN_BY (src.price, src.ts_ms) unit_price,
    SUM(src.stock) stock,
FROM
    {{ source('staging_db','books').identifier }} src
    INNER JOIN {{ ref('d_date').identifier }} ddd ON ddd.date = CAST(TO_TIMESTAMP (src.ts_ms / 1000) AS DATE)
    INNER JOIN {{ ref('d_books').identifier }} dbk ON src.book_id = dbk.book_nk
GROUP BY
    ddd.date_sk,
    dbk.book_sk