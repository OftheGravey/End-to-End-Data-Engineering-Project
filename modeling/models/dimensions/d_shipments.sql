SELECT
    -- SCD 0
    shipment_id,
    tracking_number,
    gen_random_uuid() AS shipment_sk
FROM
    {{ source('landing_db','shipments') }}
