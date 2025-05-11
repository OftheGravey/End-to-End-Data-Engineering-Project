CREATE TABLE
    IF NOT EXISTS d_orders (
        SELECT
            get_random_uuid () AS order_sk,
            order_id AS order_id,
            ARBITRARY (customer_id) AS customer_id,
            MIN(ts_ms) AS created
        FROM
            orders
        WHERE
            op = 'c'
        GROUP BY
            order_id
    )