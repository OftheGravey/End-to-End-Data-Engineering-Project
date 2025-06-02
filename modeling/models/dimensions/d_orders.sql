SELECT
    -- SCD 0
    order_id,
    -- SCD 2
    status,
    shipping_method,
    order_date,
    gen_random_uuid() AS order_sk,
    to_timestamp(ts_ms / 1000) AS valid_from,
    CASE
        WHEN op != 'd' THEN to_timestamp((ts_ms + 1) / 1000)
        WHEN lead(ts_ms + 1) OVER scd2 IS NULL THEN TIMESTAMP '9999-12-31'
        ELSE to_timestamp(lead(ts_ms + 1) OVER scd2 / 1000)
    END AS valid_to
FROM
    {{ source('landing_db','orders') }}
WINDOW
    scd2 AS (
        PARTITION BY
            order_id
        ORDER BY
            ts_ms
    )
