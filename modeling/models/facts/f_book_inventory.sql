SELECT
    ddd.date_sk,
    dbk.book_sk,
    MIN_BY(src.price, src.ts_ms) AS unit_price,
    SUM(src.stock) AS stock
FROM
    {{ source('staging_db','books').identifier }} AS src
INNER JOIN
    {{ ref('d_date').identifier }} AS ddd
    ON ddd.date = CAST(TO_TIMESTAMP(src.ts_ms / 1000) AS DATE)
INNER JOIN {{ ref('d_books').identifier }} AS dbk ON src.book_id = dbk.book_nk
GROUP BY
    ddd.date_sk,
    dbk.book_sk
