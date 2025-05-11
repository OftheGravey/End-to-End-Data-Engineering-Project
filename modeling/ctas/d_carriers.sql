CREATE TABLE
    IF NOT EXISTS d_carriers AS (
        SELECT
            gen_random_uuid () AS carrier_sk,
            carrier_id AS carrier_id,
            ARBITRARY (name) AS name,
            ARBITRARY (contact_email) AS contact_email,
            ARBITRARY (phone) AS phone,
            ARBITRARY (tracking_url_template) AS tracking_url_template,
            MIN(ts_ms) AS created
        FROM
            carriers
        WHERE
            op = 'c'
        GROUP BY
            carrier_id
    );