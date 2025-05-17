CREATE TABLE
    IF NOT EXISTS d_customers AS (
        SELECT
            gen_random_uuid () AS customer_sk,
            -- SCD 0
            customer_id,
            -- SCD 1
            first_name,
            last_name,
            -- SCD 2
            email,
            phone,
            street_address,
            city,
            state,
            postal_code,
            country,
            -- SCD 2 Helper cols
            TO_TIMESTAMP(ts_ms/1000) valid_from,
            CASE
                WHEN op = 'd' THEN TO_TIMESTAMP (ts_ms / 1000)
                WHEN LEAD (ts_ms) OVER scd2 IS NULL THEN TIMESTAMP '9999-12-31'
                ELSE TO_TIMESTAMP (LEAD (ts_ms + 1) OVER scd2 / 1000) 
            END valid_to
        FROM
            customers
        WINDOW
            scd2 AS (
                PARTITION BY
                    customer_id
                ORDER BY
                    ts_ms
            )
    )