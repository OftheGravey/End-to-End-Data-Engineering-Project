CREATE TABLE
    IF NOT EXISTS r_customer_events AS (
        SELECT
            get_random_uuid () AS customer_event_sk,
            customer_id,
            op,
            ts_ms,
            -- mutable attributes for customers
            first_name,
            last_name,
            email,
            phone,
            created_at,
            street_address,
            city,
            state,
            postal_code,
            country
        FROM
            customers
    )