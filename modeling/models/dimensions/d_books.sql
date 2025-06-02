WITH joined_data AS (
    SELECT
        bks.book_id AS book_nk,
        aus.author_id AS author_nk,
        bks.title,
        bks.isbn,
        bks.published_date,
        bks.genre,
        bks.ts_ms AS bks_ts_ms,
        aus.first_name,
        aus.last_name,
        aus.country,
        aus.ts_ms AS aus_ts_ms,
        ROW_NUMBER()
            OVER (
                PARTITION BY bks.book_id
                ORDER BY bks.ts_ms
            )
        AS book_rn,
        ROW_NUMBER()
            OVER (
                PARTITION BY aus.author_id
                ORDER BY aus.ts_ms
            )
        AS author_rn
    FROM
        {{ source('landing_db','books') }} AS bks
    INNER JOIN
        {{ source('landing_db','authors') }} AS aus
        ON bks.author_id = aus.author_id
)

SELECT
    book_nk,
    author_nk,
    GEN_RANDOM_UUID() AS book_sk,
    MAX(CASE WHEN book_rn = 1 THEN title END) AS title,
    MAX(CASE WHEN book_rn = 1 THEN isbn END) AS isbn,
    MAX(CASE WHEN book_rn = 1 THEN published_date END) AS published_date,
    MAX(CASE WHEN book_rn = 1 THEN genre END) AS genre,
    MIN(TO_TIMESTAMP(bks_ts_ms / 1000)) AS book_added,
    MAX(CASE WHEN author_rn = 1 THEN first_name END) AS author_first_name,
    MAX(CASE WHEN author_rn = 1 THEN last_name END) AS author_last_name,
    MAX(CASE WHEN author_rn = 1 THEN country END) AS author_country

FROM joined_data
GROUP BY book_nk, author_nk
