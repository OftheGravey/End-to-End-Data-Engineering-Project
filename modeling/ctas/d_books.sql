CREATE TABLE IF NOT EXISTS d_books AS (
    SELECT 
    gen_random_uuid() book_sk,
    book_id,
    ARBITRARY(title) AS title,
    ARBITRARY(author_id) AS author_id,
    ARBITRARY(isbn) AS isbn,
    ARBITRARY(published_date) AS published_date,
    ARBITRARY(description) AS description,
    ARBITRARY(genre) AS genre,
    MIN(ts_ms) added
    FROM books 
    WHERE op = 'c' 
    GROUP BY book_id
);