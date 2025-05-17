-- AUTHORS
CREATE TABLE authors (
    author_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    biography TEXT,
    country VARCHAR(100)
);
ALTER TABLE authors REPLICA IDENTITY FULL;

-- BOOKS
CREATE TABLE books (
    book_id SERIAL PRIMARY KEY,
    title VARCHAR(150) NOT NULL,
    author_id INT NOT NULL REFERENCES authors(author_id) ON DELETE CASCADE,
    isbn VARCHAR(17) UNIQUE NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    published_date DATE,
    description TEXT,
    genre VARCHAR(50),
    stock INT NOT NULL DEFAULT 0 CHECK (stock >= 0)
);
ALTER TABLE books REPLICA IDENTITY FULL;

-- CUSTOMERS
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    street_address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(20),
    country VARCHAR(100)
);
ALTER TABLE customers REPLICA IDENTITY FULL;

-- ORDERS
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(customer_id) ON DELETE CASCADE,
    order_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) NOT NULL CHECK (status IN ('pending', 'shipped', 'delivered', 'cancelled')),
    shipping_method VARCHAR(50),
    notes TEXT
);
ALTER TABLE orders REPLICA IDENTITY FULL;

-- ORDER ITEMS
CREATE TABLE order_items (
    order_item_id SERIAL PRIMARY KEY,
    order_id INT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
    book_id INT NOT NULL REFERENCES books(book_id),
    quantity INT NOT NULL CHECK (quantity > 0),
    price_at_purchase NUMERIC(10,2) NOT NULL CHECK (price_at_purchase >= 0),
    discount NUMERIC(5,2) DEFAULT 0 CHECK (discount >= 0),
    comment TEXT,
    UNIQUE(order_id, book_id)
);
ALTER TABLE order_items REPLICA IDENTITY FULL;