WITH base_data AS (
    SELECT
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
        -- Insert time
        ts_ms
    FROM {{ source('landing_db','customers') }}
),

scd2_with_row_numbers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY ts_ms
        ) AS rn,
        LAG(email) OVER (
            PARTITION BY customer_id
            ORDER BY ts_ms
        ) AS prev_email,
        LAG(phone) OVER (
            PARTITION BY customer_id
            ORDER BY ts_ms
        ) AS prev_phone,
        LAG(street_address)
            OVER (
                PARTITION BY customer_id
                ORDER BY ts_ms
            )
        AS prev_street_address,
        LAG(city) OVER (
            PARTITION BY customer_id
            ORDER BY ts_ms
        ) AS prev_city,
        LAG(state) OVER (
            PARTITION BY customer_id
            ORDER BY ts_ms
        ) AS prev_state,
        LAG(postal_code)
            OVER (
                PARTITION BY customer_id
                ORDER BY ts_ms
            )
        AS prev_postal_code,
        LAG(country)
            OVER (
                PARTITION BY customer_id
                ORDER BY ts_ms
            )
        AS prev_country,
        LEAD(ts_ms) OVER (
            PARTITION BY customer_id
            ORDER BY ts_ms
        ) AS next_ts,
        FIRST_VALUE(first_name)
            OVER (
                PARTITION BY customer_id
                ORDER BY ts_ms DESC
            )
        AS latest_first_name,
        FIRST_VALUE(last_name)
            OVER (
                PARTITION BY customer_id
                ORDER BY ts_ms DESC
            )
        AS latest_last_name
    FROM base_data
)

SELECT
    customer_id,
    -- SCD 0
    email,
    -- SCD 1
    phone,
    street_address,
    -- SCD 2
    city,
    state,
    postal_code,
    country,
    GEN_RANDOM_UUID() AS customer_sk,
    MAX(latest_first_name) AS first_name,
    MAX(latest_last_name) AS last_name,
    -- SCD 2 Helper cols
    TO_TIMESTAMP(MIN(ts_ms) / 1000) AS valid_from,
    TO_TIMESTAMP(COALESCE(MIN(next_ts) / 1000, 253402300799)) AS valid_to
FROM scd2_with_row_numbers
WHERE
    rn = 1 OR (
        email IS DISTINCT FROM prev_email
        OR phone IS DISTINCT FROM prev_phone
        OR street_address IS DISTINCT FROM prev_street_address
        OR city IS DISTINCT FROM prev_city
        OR state IS DISTINCT FROM prev_state
        OR postal_code IS DISTINCT FROM prev_postal_code
        OR country IS DISTINCT FROM prev_country
    )
GROUP BY
    customer_id, email,
    phone,
    street_address,
    city,
    state,
    postal_code,
    country
