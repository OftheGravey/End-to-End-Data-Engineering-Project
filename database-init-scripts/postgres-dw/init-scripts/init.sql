CREATE SCHEMA modeling_db;

CREATE DATABASE dagster;


-- Dimension Tables:
CREATE TABLE
    IF NOT EXISTS modeling_db.d_orders (
        orderSk VARCHAR PRIMARY KEY,
        orderId INTEGER,
        status VARCHAR,
        shippingMethod VARCHAR,
        orderDate DATE,
        validFrom BIGINT,
        validTo BIGINT
    );

CREATE TABLE
    IF NOT EXISTS modeling_db.d_customers (
        customerSk TEXT PRIMARY KEY,
        customerId INTEGER,
        firstName TEXT NOT NULL,
        lastName TEXT NOT NULL,
        email TEXT NOT NULL,
        phone TEXT,
        streetAddress TEXT,
        city TEXT,
        state TEXT,
        postalCode TEXT,
        country TEXT,
        validFrom BIGINT NOT NULL,
        validTo BIGINT NOT NULL
    );

CREATE TABLE IF NOT EXISTS
    modeling_db.d_books (
        bookSk VARCHAR PRIMARY KEY,
        bookId INTEGER, 
        authorId INTEGER,
        title VARCHAR,
        isbn VARCHAR,
        publishedDate DATE,
        genre VARCHAR,
        authorFirstName VARCHAR,
        authorLastName VARCHAR,
        authorCountry VARCHAR,
        validFrom BIGINT NOT NULL,
        validTo BIGINT NOT NULL
    );

CREATE TABLE IF NOT EXISTS
    modeling_db.f_order_items (
        orderItemSk VARCHAR PRIMARY KEY,
        orderSk VARCHAR,
        bookSk VARCHAR,
        customerSk VARCHAR,
        orderItemId INT,
        quantity INT,
        priceAtPurchase FLOAT,
        discount FLOAT,
        transactionTime BIGINT,
        priceTotal FLOAT
    );