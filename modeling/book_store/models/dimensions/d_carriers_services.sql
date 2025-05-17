SELECT
    gen_random_uuid () AS carrier_service_sk,
    -- SCD 0
    car.carrier_id AS carrier_id,
    ser.service_id,
    -- SCD 1
    car.name AS carrier_name,
    car.contact_email AS carrier_contact_email,
    car.phone AS carrier_phone,
    car.tracking_url_template AS carrier_tracking_url_template,
    ser.service_name,
    -- SCD 2
    ser.estimated_days,
    ser.cost_estimate,
    TO_TIMESTAMP (LEAST (ser.ts_ms, car.ts_ms)) valid_from,
    CASE
        WHEN ser.op != 'd'
        AND car.op != 'd' THEN TO_TIMESTAMP (
            LEAST (
                LEAD (ser.ts_ms + 1) OVER scd2_ser,
                LEAD (car.ts_ms + 1) OVER scd2_car
            ) / 1000
        )
        ELSE TO_TIMESTAMP (ser.ts_ms / 1000)
    END valid_to
FROM
    {{ source('staging_db','carriers').identifier }} car
    INNER JOIN {{ source('staging_db','shipping_services').identifier }} ser ON car.carrier_id = ser.carrier_id
    and car.ts_ms <= ser.ts_ms
WHERE ser.op != 'd' 
    AND ser.op IN ('c', 'u')
    AND car.op IN ('c', 'u')
WINDOW
    scd2_ser AS (
        PARTITION BY
            ser.service_id
        ORDER BY
            ser.ts_ms
    ),
    scd2_car AS (
        PARTITION BY
            car.carrier_id
        ORDER BY
            car.ts_ms
    )