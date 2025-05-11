CREATE TABLE
    IF NOT EXISTS f_shipment_state (
        SELECT
            get_random_uuid () AS shipment_state_sk,
            shipment_id,
            carrier_id,
            order_id,
            service_id,
            tracking_number,
            shipping_status,
            shipped_date,
            expected_delivery_date,
            actual_delivery_date,
            shipping_cost,
            op
        FROM
            shipment
    );