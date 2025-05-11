CREATE TABLE IF NOT EXISTS f_shipping_services_events (
    service_event_sk INTEGER,
    service_id INTEGER, 
    carrier_id INTEGER, 
    estimated_days INTEGER,
    cost_estimate DECIMAL(10, 2),
    op TEXT,
    ts_ms BIGINT
)