CREATE TABLE
    IF NOT EXISTS d_books AS (
        SELECT
            gen_random_uuid () book_sk,
            -- SCD 0
            bks.book_id AS book_nk,
            aus.author_id author_nk,
            MIN_BY(title, bks.ts_ms) AS title,
            MIN_BY(isbn, bks.ts_ms) AS isbn,
            MIN_BY(published_date, bks.ts_ms) AS published_date,
            MIN_BY(genre, bks.ts_ms) AS genre,
            MIN(TO_TIMESTAMP(bks.ts_ms/1000)) book_added,
            MIN_BY(aus.first_name, aus.ts_ms) AS author_first_name,
            MIN_BY(aus.last_name, aus.ts_ms) AS author_last_name,
            MIN_BY(aus.country, aus.ts_ms) AS author_country
        FROM
            books bks
        INNER JOIN authors aus ON bks.author_id = aus.author_id 
        GROUP BY bks.book_id, aus.author_id
    );