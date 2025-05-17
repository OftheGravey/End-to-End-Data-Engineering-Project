CREATE TABLE
    IF NOT EXISTS f_book_inventory AS (
        SELECT
            ddd.date_sk
            dbk.book_sk,
            MAX(src.price) unit_price
            SUM(src.stock) stock,
        FROM
            books src
            INNER JOIN d_date ddd ON ddd.date = CAST(TO_TIMESTAMP(src.ts_ms/1000) AS DATE)
            INNER JOIN d_books dbk ON src.book_id = dbk.book_nk
        GROUP BY ddd.date_sk, dbk.book_sk
    )