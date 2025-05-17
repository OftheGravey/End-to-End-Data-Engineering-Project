CREATE TABLE
    IF NOT EXISTS f_book_inventory AS (
        SELECT
            dbk.book_sk,
            src.stock,
            src.price,
            src.op,
            TO_TIMESTAMP(src.ts_ms/1000) transaction_time
        FROM
            books src
            INNER JOIN d_books dbk ON src.book_id = dbk.book_nk
    )