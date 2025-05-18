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
    FROM
        {{ source('staging_db','customers').identifier }}
),

scd2_with_row_numbers AS (
    SELECT
        *,
        ROW_NUMBER() OVER scd2 AS rn,
        LAG(email) OVER scd2 AS prev_email,
        LAG(phone) OVER scd2 AS prev_phone,
        LAG(street_address) OVER scd2 AS prev_street_address,
        LAG(city) OVER scd2 AS prev_city,
        LAG(state) OVER scd2 AS prev_state,
        LAG(postal_code) OVER scd2 AS prev_postal_code,
        LAG(country) OVER scd2 AS prev_country,
        LEAD(ts_ms) OVER scd2 AS next_ts
    FROM base_data
    WINDOW scd2 AS (
        PARTITION BY customer_id
        ORDER BY ts_ms
    )
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
    MAX_BY(first_name, ts_ms) AS first_name,
    MAX_BY(last_name, ts_ms) AS last_name,
    -- SCD 2 Helper cols
    TO_TIMESTAMP(MIN(ts_ms) / 1000) AS valid_from,
    TO_TIMESTAMP(COALESCE(MIN(next_ts) / 1000, 253402300799)) AS valid_to
FROM
    scd2_with_row_numbers
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
