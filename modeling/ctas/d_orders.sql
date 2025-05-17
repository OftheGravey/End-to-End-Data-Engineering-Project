CREATE TABLE
    IF NOT EXISTS d_orders AS (
        SELECT
            gen_random_uuid () AS order_sk,
            -- SCD 0
            order_id,
            -- SCD 1
            status,
            shipping_method,
            order_date,
            TO_TIMESTAMP(ts_ms/1000) valid_from,
            CASE
                WHEN op != 'd' THEN TO_TIMESTAMP((ts_ms + 1)/1000)
                WHEN LEAD (ts_ms + 1) OVER scd2 IS NULL THEN TIMESTAMP '9999-12-31'
                ELSE TO_TIMESTAMP(LEAD (ts_ms + 1) OVER scd2 / 1000)
            END valid_to
        FROM
            orders
        WINDOW
            scd2 AS (
                PARTITION BY
                    customer_id
                ORDER BY
                    ts_ms
            )
    );