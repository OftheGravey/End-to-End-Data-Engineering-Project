CREATE TABLE IF NOT EXISTS d_authors AS (
    -- get dimension from author creation
    SELECT gen_random_uuid() author_sk,
        author_id,
        arbitrary(first_name) first_name,
        arbitrary(last_name) last_name,
        arbitrary(biography) biography,
        arbitrary(country) country,
        MIN(ts_ms) added
    FROM authors
    WHERE op = 'c'
    GROUP BY author_id
)