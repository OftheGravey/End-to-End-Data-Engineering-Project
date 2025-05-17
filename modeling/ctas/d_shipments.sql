CREATE TABLE
    IF NOT EXISTS d_shipments AS (
        SELECT
            -- SCD 1
            gen_random_uuid () AS shipment_sk,
            shipment_id,
            tracking_number
        FROM
            shipments
    );