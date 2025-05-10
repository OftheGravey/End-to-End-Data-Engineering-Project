-- Insert authors and capture IDs
WITH inserted_authors AS (
  INSERT INTO authors (first_name, last_name, biography, country)
  VALUES
    ('Jane', 'Austen', 'English novelist known primarily for her six major novels.', 'United Kingdom'),
    ('George', 'Orwell', 'Author of dystopian classics including 1984 and Animal Farm.', 'United Kingdom'),
    ('Haruki', 'Murakami', 'Contemporary Japanese writer known for surreal fiction.', 'Japan')
  RETURNING author_id, last_name
),

-- Insert books using author IDs
inserted_books AS (
  INSERT INTO books (title, author_id, isbn, price, published_date, description, genre, stock)
  SELECT *
  FROM (
    VALUES
      ('Sense and Sensibility', (SELECT author_id FROM inserted_authors WHERE last_name = 'Austen'), '9780141199672', 8.99, '1811-10-30'::date,
       'A story of love, heartbreak, and societal expectations.', 'Romance', 20),
      ('1984', (SELECT author_id FROM inserted_authors WHERE last_name = 'Orwell'), '9780451524935', 9.99, '1949-06-08'::date,
       'A dystopian novel set in a totalitarian society ruled by Big Brother.', 'Dystopian', 15),
      ('Kafka on the Shore', (SELECT author_id FROM inserted_authors WHERE last_name = 'Murakami'), '9781400079278', 12.50, '2002-09-12'::date,
       'A surreal novel blending reality and metaphysical concepts.', 'Fantasy', 12)
  ) AS b(title, author_id, isbn, price, published_date, description, genre, stock)
  RETURNING book_id, title
),

-- Insert customers and capture IDs
inserted_customers AS (
  INSERT INTO customers (first_name, last_name, email, phone, street_address, city, state, postal_code, country)
  VALUES 
    ('Clara', 'Nguyen', 'clara@example.com', '555-444-3333', '789 Pine St', 'Portland', 'OR', '97205', 'USA'),
    ('Liam', 'O’Connor', 'liam.oconnor@example.com', '555-123-4567', '123 Elm St', 'Boston', 'MA', '02118', 'USA')
  RETURNING customer_id, last_name
),

-- Insert orders and capture IDs
inserted_orders AS (
  INSERT INTO orders (customer_id, order_date, status, shipping_method, notes)
  SELECT *
  FROM (
    VALUES
      ((SELECT customer_id FROM inserted_customers WHERE last_name = 'Nguyen'), NOW(), 'pending', 'Standard', 'Gift wrap requested.'),
      ((SELECT customer_id FROM inserted_customers WHERE last_name = 'O’Connor'), NOW(), 'shipped', 'Express', 'Deliver before weekend.')
  ) AS o(customer_id, order_date, status, shipping_method, notes)
  RETURNING order_id, customer_id
)

-- Final step: Insert order items using the order and book IDs
INSERT INTO order_items (order_id, book_id, quantity, price_at_purchase, discount, comment)
SELECT *
FROM (
  VALUES
    (
      (SELECT order_id FROM inserted_orders WHERE customer_id = (SELECT customer_id FROM inserted_customers WHERE last_name = 'Nguyen')),
      (SELECT book_id FROM inserted_books WHERE title = 'Sense and Sensibility'),
      1, 8.99, 0.00, 'Bought for book club.'
    ),
    (
      (SELECT order_id FROM inserted_orders WHERE customer_id = (SELECT customer_id FROM inserted_customers WHERE last_name = 'Nguyen')),
      (SELECT book_id FROM inserted_books WHERE title = '1984'),
      2, 9.99, 1.00, 'Special discount applied.'
    ),
    (
      (SELECT order_id FROM inserted_orders WHERE customer_id = (SELECT customer_id FROM inserted_customers WHERE last_name = 'O’Connor')),
      (SELECT book_id FROM inserted_books WHERE title = 'Kafka on the Shore'),
      1, 12.50, 0.00, NULL
    )
) AS oi(order_id, book_id, quantity, price_at_purchase, discount, comment);

-- Insert more authors
WITH new_authors AS (
  INSERT INTO authors (first_name, last_name, biography, country)
  VALUES
    ('Mary', 'Shelley', 'Author of Frankenstein and pioneer of science fiction.', 'United Kingdom'),
    ('Chinua', 'Achebe', 'Author of "Things Fall Apart" and father of African literature.', 'Nigeria')
  RETURNING author_id, last_name
),

-- Insert more books
new_books AS (
  INSERT INTO books (title, author_id, isbn, price, published_date, description, genre, stock)
  SELECT *
  FROM (
    VALUES
      ('Frankenstein', (SELECT author_id FROM new_authors WHERE last_name = 'Shelley'), '9780141439471', 10.99, '1818-01-01'::date,
       'A gothic novel about Victor Frankenstein and his creature.', 'Horror', 30),
      ('Things Fall Apart', (SELECT author_id FROM new_authors WHERE last_name = 'Achebe'), '9780385474542', 11.50, '1958-06-17'::date,
       'Story of pre- and post-colonial life in Nigeria.', 'Historical Fiction', 18),
      ('Animal Farm', (SELECT author_id FROM authors WHERE last_name = 'Orwell'), '9780451526342', 7.50, '1945-08-17'::date,
       'A satirical allegory of Soviet totalitarianism.', 'Political Satire', 25)
  ) AS b(title, author_id, isbn, price, published_date, description, genre, stock)
  RETURNING book_id, title
),

-- Insert more customers
new_customers AS (
  INSERT INTO customers (first_name, last_name, email, phone, street_address, city, state, postal_code, country)
  VALUES 
    ('Amina', 'Yusuf', 'amina.yusuf@example.com', '555-234-7890', '456 Maple Ave', 'Chicago', 'IL', '60616', 'USA'),
    ('Carlos', 'Martinez', 'carlos.martinez@example.com', '555-987-6543', '101 Sunset Blvd', 'Los Angeles', 'CA', '90028', 'USA')
  RETURNING customer_id, last_name
),

-- Insert new orders
new_orders AS (
  INSERT INTO orders (customer_id, order_date, status, shipping_method, notes)
  SELECT *
  FROM (
    VALUES
      ((SELECT customer_id FROM new_customers WHERE last_name = 'Yusuf'), NOW(), 'pending', 'Standard', 'Send with eco-friendly packaging.'),
      ((SELECT customer_id FROM new_customers WHERE last_name = 'Martinez'), NOW(), 'shipped', 'Two-Day', 'First-time customer.')
  ) AS o(customer_id, order_date, status, shipping_method, notes)
  RETURNING order_id, customer_id
)

-- Insert new order items
INSERT INTO order_items (order_id, book_id, quantity, price_at_purchase, discount, comment)
SELECT *
FROM (
  VALUES
    (
      (SELECT order_id FROM new_orders WHERE customer_id = (SELECT customer_id FROM new_customers WHERE last_name = 'Yusuf')),
      (SELECT book_id FROM new_books WHERE title = 'Frankenstein'),
      1, 10.99, 0.00, 'Classic horror pick.'
    ),
    (
      (SELECT order_id FROM new_orders WHERE customer_id = (SELECT customer_id FROM new_customers WHERE last_name = 'Yusuf')),
      (SELECT book_id FROM new_books WHERE title = 'Things Fall Apart'),
      1, 11.50, 0.50, 'Studying for African literature course.'
    ),
    (
      (SELECT order_id FROM new_orders WHERE customer_id = (SELECT customer_id FROM new_customers WHERE last_name = 'Martinez')),
      (SELECT book_id FROM new_books WHERE title = 'Animal Farm'),
      2, 7.50, 0.00, 'Gifted to a colleague.'
    )
) AS oi(order_id, book_id, quantity, price_at_purchase, discount, comment);


-- ✅ INSERT: New customer joins
INSERT INTO customers (first_name, last_name, email, phone, street_address, city, state, postal_code, country)
VALUES ('Clara', 'Nguyen', 'clara@example.com', '555-444-3333', '789 Pine St', 'Portland', 'OR', '97205', 'USA');

-- ✅ INSERT: New book by Jane Austen
INSERT INTO books (title, author_id, isbn, price, published_date, description, genre, stock)
VALUES (
  'Sense and Sensibility', 2, '9780141199672', 8.99, '1811-10-30',
  'A story of love, heartbreak, and societal expectations.',
  'Romance', 20
);

-- ✅ INSERT: Clara places an order
INSERT INTO orders (customer_id, status, shipping_method, notes)
VALUES (3, 'pending', 'standard', 'First-time buyer discount');

-- ✅ INSERT: Order items for Clara's order (assumes order_id = 3)
INSERT INTO order_items (order_id, book_id, quantity, price_at_purchase, discount, comment)
VALUES
(3, 5, 1, 8.99, 1.00, '10% off first order'),
(3, 3, 1, 7.99, 0, 'For gift');

-- 🔄 UPDATE: Reduce stock after order placed
UPDATE books SET stock = stock - 1 WHERE book_id IN (5, 3);

-- 🔄 UPDATE: Mark Alice’s order as shipped
UPDATE orders SET status = 'shipped' WHERE order_id = 1;

-- 🔄 UPDATE: Change Bob’s phone number
UPDATE customers SET phone = '111-222-3333' WHERE email = 'bob@example.com';

-- 🔄 UPDATE: Increase price of “Kafka on the Shore”
UPDATE books SET price = price + 1.50 WHERE title = 'Kafka on the Shore';

-- 🗑️ DELETE: Cancel and delete Clara's order (cascade will delete order_items too)
DELETE FROM orders WHERE order_id = 3;

-- 🔄 UPDATE: Increase stock back since Clara’s order was cancelled
UPDATE books SET stock = stock + 1 WHERE book_id IN (5, 3);

-- ✅ INSERT: Another customer and order
INSERT INTO customers (first_name, last_name, email, phone, street_address, city, state, postal_code, country)
VALUES ('David', 'Lee', 'david.lee@example.com', '222-333-4444', '123 Birch Ln', 'Seattle', 'WA', '98101', 'USA');

INSERT INTO orders (customer_id, status, shipping_method, notes)
VALUES (4, 'pending', 'express', NULL);

INSERT INTO order_items (order_id, book_id, quantity, price_at_purchase, discount, comment)
VALUES (4, 4, 1, 16.00, 0.50, 'Holiday promo');
