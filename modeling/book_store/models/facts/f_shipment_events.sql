SELECT
    dsh.shipment_sk,
    dor.order_sk,
    dcr.carrier_service_sk,
    src.tracking_number,
    src.shipping_cost,
    src.shipping_status,
    src.shipped_date,
    src.expected_delivery_date,
    src.actual_delivery_date,
    gen_random_uuid() AS shipment_event_sk,
    to_timestamp(src.ts_ms / 1000) AS transaction_time
FROM
    {{ source('staging_db','shipments').identifier }} AS src
INNER JOIN
    {{ ref('d_shipments').identifier }} AS dsh
    ON src.shipment_id = dsh.shipment_id
INNER JOIN {{ ref('d_orders').identifier }} AS dor
    ON
        src.order_id = dor.order_id
        AND to_timestamp(src.ts_ms / 1000) >= dor.valid_from
        AND to_timestamp(src.ts_ms / 1000) < dor.valid_to
INNER JOIN {{ ref('d_carriers_services').identifier }} AS dcr
    ON
        src.carrier_id = dcr.carrier_id
        AND src.service_id = dcr.service_id
        AND to_timestamp(src.ts_ms / 1000) >= dcr.valid_from
        AND to_timestamp(src.ts_ms / 1000) < dcr.valid_to
INNER JOIN
    {{ ref('d_date').identifier }} AS ddd
    ON ddd.date = cast(to_timestamp(src.ts_ms / 1000) AS DATE)
