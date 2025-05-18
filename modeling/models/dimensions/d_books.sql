SELECT
    bks.book_id AS book_nk,
    -- SCD 0
    aus.author_id AS author_nk,
    gen_random_uuid() AS book_sk,
    min_by(bks.title, bks.ts_ms) AS title,
    min_by(bks.isbn, bks.ts_ms) AS isbn,
    min_by(bks.published_date, bks.ts_ms) AS published_date,
    min_by(bks.genre, bks.ts_ms) AS genre,
    min(to_timestamp(bks.ts_ms / 1000)) AS book_added,
    min_by(aus.first_name, aus.ts_ms) AS author_first_name,
    min_by(aus.last_name, aus.ts_ms) AS author_last_name,
    min_by(aus.country, aus.ts_ms) AS author_country
FROM
    {{ source('staging_db','books').identifier }} AS bks
INNER JOIN
    {{ source('staging_db','authors').identifier }} AS aus
    ON bks.author_id = aus.author_id
GROUP BY
    bks.book_id,
    aus.author_id
