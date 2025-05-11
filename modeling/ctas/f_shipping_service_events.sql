CREATE TABLE
    IF NOT EXISTS f_shipping_services_events AS (
        SELECT
            get_random_uuid () AS service_event_sk,
            service_id,
            carrier_id,
            estimated_days,
            cost_estimate,
            op,
            ts_ms
        FROM
            shipping_services
    );