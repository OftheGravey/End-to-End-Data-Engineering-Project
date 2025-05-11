CREATE TABLE
    IF NOT EXISTS d_shipments AS (
        SELECT
            get_random_uuid () AS shipment_sk,
            shipment_id AS shipment_id,
            ARBITRARY (carrier_id) AS carrier_id,
            ARBITRARY (service_id) AS service_id,
            MIN(ts_ms) AS created
        FROM
            shipments
        WHERE
            op = 'c'
        GROUP BY
            shipment_id
    );