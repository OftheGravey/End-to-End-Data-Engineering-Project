CREATE TABLE IF NOT EXISTS d_authors (
    author_sk INTEGER,
    author_id INTEGER,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    biography TEXT,
    country TEXT,
    added BIGINT
);