CREATE TABLE
    IF NOT EXISTS f_shipment_events AS (
        SELECT
            get_random_uuid () AS shipment_event_sk,
            shipment_event_id,
            -- measurements
            status,
            location,
            event_time,
            notes,
            op,
            ts_ms,
            status,
            shipping_method
        FROM
            shipment_events
    );