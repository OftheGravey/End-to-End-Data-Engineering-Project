CREATE TABLE IF NOT EXISTS d_books (
    book_sk INTEGER,
    book_id INTEGER,
    title TEXT NOT NULL,
    author_id INTEGER NOT NULL,
    isbn TEXT NOT NULL,
    published_date DATE,
    description TEXT,
    genre TEXT,
    added BIGINT
);