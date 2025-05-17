CREATE TABLE
    IF NOT EXISTS d_carrier_services AS (
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
            TO_TIMESTAMP(LEAST (ser.ts_ms, car.ts_ms)) valid_from,
            CASE
                WHEN ser.op != 'd'
                AND car.op != 'd' THEN TO_TIMESTAMP(LEAST (
                    LEAD (ser.ts_ms + 1) OVER scd2_ser,
                    LEAD (car.ts_ms + 1) OVER scd2_car
                ) / 1000)
                ELSE TO_TIMESTAMP(ser.ts_ms/1000)
            END valid_to
        FROM
            carriers car
            INNER JOIN shipping_services ser ON car.carrier_id = ser.carrier_id
            and car.ts_ms <= ser.ts_ms
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
    );