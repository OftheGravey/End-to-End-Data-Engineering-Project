WITH enriched_data AS (
    SELECT
        ddd.date_sk,
        dbk.book_sk,
        src.price,
        src.stock,
        src.ts_ms,
        ROW_NUMBER() OVER (
            PARTITION BY ddd.date_sk, dbk.book_sk
            ORDER BY src.ts_ms
        ) AS rn
    FROM
        {{ source('landing_db','books') }} AS src
    INNER JOIN {{ ref('d_date') }} AS ddd
        ON ddd.date = CAST(TO_TIMESTAMP(src.ts_ms / 1000) AS DATE)
    INNER JOIN {{ ref('d_books') }} AS dbk
        ON src.book_id = dbk.book_nk
)

SELECT
    date_sk,
    book_sk,
    MAX(CASE WHEN rn = 1 THEN price END) AS unit_price,
    SUM(stock) AS stock
FROM enriched_data
GROUP BY date_sk, book_sk
