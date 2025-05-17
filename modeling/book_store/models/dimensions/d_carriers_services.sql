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
  FROM {{ source('staging_db','carriers').identifier }} car
  JOIN {{ source('staging_db','shipping_services').identifier }} ser
    ON car.carrier_id = ser.carrier_id
  WHERE car.op IN ('c', 'u')
    AND ser.op IN ('c', 'u')
),

scd2_with_row_numbers AS (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY carrier_id, service_id
      ORDER BY ts_ms
    ) AS rn,
    LAG(estimated_days) OVER (PARTITION BY carrier_id, service_id ORDER BY ts_ms) AS prev_estimated_days,
    LAG(cost_estimate) OVER (PARTITION BY carrier_id, service_id ORDER BY ts_ms) AS prev_cost_estimate,
    LEAD(ts_ms) OVER (PARTITION BY carrier_id, service_id ORDER BY ts_ms) AS next_ts
  FROM base_data
)
  SELECT
    gen_random_uuid() AS carrier_service_sk,
    -- SCD 0
    carrier_id,
    service_id,
    -- SCD 1
    MAX_BY(carrier_name, ts_ms) AS carrier_name,
    MAX_BY(contact_email, ts_ms) AS carrier_contact_email,
    MAX_BY(phone, ts_ms) AS carrier_phone,
    MAX_BY(tracking_url_template, ts_ms) AS carrier_tracking_url_template,
    MAX_BY(service_name, ts_ms) AS service_name,
    -- SCD 2
    estimated_days,
    cost_estimate,
    TO_TIMESTAMP(MIN(ts_ms) / 1000) AS valid_from,
    TO_TIMESTAMP(COALESCE(MIN(next_ts) / 1000, 253402300799)) AS valid_to
  FROM scd2_with_row_numbers
  WHERE rn = 1 OR estimated_days IS DISTINCT FROM prev_estimated_days OR cost_estimate IS DISTINCT FROM prev_cost_estimate
    GROUP BY
    carrier_id,
    service_id,
    estimated_days,
    cost_estimate