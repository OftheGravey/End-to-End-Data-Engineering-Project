SELECT
    gen_random_uuid () AS shipment_event_sk,
    dsh.shipment_sk,
    dor.order_sk,
    dcr.carrier_service_sk,
    TO_TIMESTAMP (src.ts_ms / 1000) transaction_time,
    src.tracking_number,
    src.shipping_cost,
    src.shipping_status,
    src.shipped_date,
    src.expected_delivery_date,
    src.actual_delivery_date
FROM
    {{ source('staging_db','shipments').identifier }} src
    INNER JOIN {{ ref('d_shipments').identifier }} dsh ON src.shipment_id = dsh.shipment_id
    INNER JOIN {{ ref('d_orders').identifier }} dor ON src.order_id = dor.order_id
    AND TO_TIMESTAMP (src.ts_ms / 1000) >= dor.valid_from
    AND TO_TIMESTAMP (src.ts_ms / 1000) < dor.valid_to
    INNER JOIN {{ ref('d_carriers_services').identifier }} dcr ON src.carrier_id = dcr.carrier_id
    AND src.service_id = dcr.service_id
    AND TO_TIMESTAMP (src.ts_ms / 1000) >= dcr.valid_from
    AND TO_TIMESTAMP (src.ts_ms / 1000) < dcr.valid_to
    INNER JOIN {{ ref('d_date').identifier }} ddd ON ddd.date = CAST(TO_TIMESTAMP (src.ts_ms / 1000) AS DATE)