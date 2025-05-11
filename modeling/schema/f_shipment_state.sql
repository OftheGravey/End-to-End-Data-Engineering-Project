CREATE TABLE IF NOT EXISTS f_shipment_state (
    shipment_state_sk INT,
    shipment_id INT,
    carrier_id INTEGER,
    order_id INTEGER,
    service_id INTEGER,
    tracking_number VARCHAR,
    shipping_status VARCHAR,
    shipped_date DATE,
    expected_delivery_date DATE,
    actual_delivery_date DATE,
    shipping_cost DECIMAL(10, 2),
    op TEXT
);