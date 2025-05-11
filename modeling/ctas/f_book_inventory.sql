CREATE TABLE
    IF NOT EXISTS f_book_inventory AS (
        SELECT
            get_random_uuid () AS book_stock_event_sk,
            book_id,
            -- measurements
            stock,
            price,
            op,
            ts_ms
        FROM
            books
    )