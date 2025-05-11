CREATE TABLE
    IF NOT EXISTS d_customers AS (
        SELECT
            get_random_uuid () AS customer_sk,
            customer_id AS customer_id,
            MIN(ts_ms) AS created
        FROM
            customers
        WHERE
            op = 'c'
        GROUP BY
            customers
    );