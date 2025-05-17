SELECT
    -- SCD 0
    gen_random_uuid () AS shipment_sk,
    shipment_id,
    tracking_number
FROM
    {{ source('staging_db','shipments').identifier }}