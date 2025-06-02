WITH base_data AS (
    SELECT
        car.carrier_id,
        ser.service_id,
        car.name AS carrier_name,
        car.contact_email,
        car.phone,
        car.tracking_url_template,
        ser.service_name,
        ser.estimated_days,
        ser.cost_estimate,
        GREATEST(car.ts_ms, ser.ts_ms) AS ts_ms
    FROM {{ source('landing_db','carriers') }} AS car
    INNER JOIN {{ source('landing_db','shipping_services') }} AS ser
        ON car.carrier_id = ser.carrier_id
    WHERE
        car.op IN ('c', 'u')
        AND ser.op IN ('c', 'u')
),

scd2_with_row_numbers AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY carrier_id, service_id
            ORDER BY ts_ms
        ) AS rn,
        LAG(estimated_days) OVER (
            PARTITION BY carrier_id, service_id
            ORDER BY ts_ms
        ) AS prev_estimated_days,
        LAG(cost_estimate) OVER (
            PARTITION BY carrier_id, service_id
            ORDER BY ts_ms
        ) AS prev_cost_estimate,
        LEAD(ts_ms) OVER (
            PARTITION BY carrier_id, service_id
            ORDER BY ts_ms
        ) AS next_ts,
        FIRST_VALUE(carrier_name) OVER (
            PARTITION BY carrier_id, service_id
            ORDER BY ts_ms DESC
        ) AS latest_carrier_name,
        FIRST_VALUE(contact_email) OVER (
            PARTITION BY carrier_id, service_id
            ORDER BY ts_ms DESC
        ) AS latest_contact_email,
        FIRST_VALUE(phone) OVER (
            PARTITION BY carrier_id, service_id
            ORDER BY ts_ms DESC
        ) AS latest_phone,
        FIRST_VALUE(tracking_url_template) OVER (
            PARTITION BY carrier_id, service_id
            ORDER BY ts_ms DESC
        ) AS latest_tracking_url_template,
        FIRST_VALUE(service_name) OVER (
            PARTITION BY carrier_id, service_id
            ORDER BY ts_ms DESC
        ) AS latest_service_name
    FROM base_data
)

SELECT
    carrier_id,
    service_id,
    estimated_days,
    cost_estimate,
    GEN_RANDOM_UUID() AS carrier_service_sk,
    MAX(latest_carrier_name) AS carrier_name,
    MAX(latest_contact_email) AS carrier_contact_email,
    MAX(latest_phone) AS carrier_phone,
    MAX(latest_tracking_url_template) AS carrier_tracking_url_template,
    MAX(latest_service_name) AS service_name,
    TO_TIMESTAMP(MIN(ts_ms) / 1000) AS valid_from,
    TO_TIMESTAMP(COALESCE(MIN(next_ts) / 1000, 253402300799)) AS valid_to

FROM scd2_with_row_numbers
WHERE
    rn = 1
    OR estimated_days IS DISTINCT FROM prev_estimated_days
    OR cost_estimate IS DISTINCT FROM prev_cost_estimate
GROUP BY
    carrier_id,
    service_id,
    estimated_days,
    cost_estimate
