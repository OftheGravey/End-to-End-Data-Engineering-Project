CREATE TABLE IF NOT EXISTS d_carriers (
    carrier_sk INTEGER,
    carrier_id INTEGER,
    name VARCHAR,
    contact_email VARCHAR,
    phone VARCHAR,
    tracking_url_template VARCHAR,
    created BIGINT
);