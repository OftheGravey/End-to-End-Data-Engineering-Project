import random
import time
from datetime import datetime, timedelta
import psycopg2
import mysql.connector

# AI Generated code for creating random inputs inserts, updates and deletes to both databases

# --- Constants ---
PG_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "postgres",
    "database": "store_db"
}

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": "debezium",
    "password": "dbz",
    "database": "shipping_db"
}

# --- Sample Data ---
FIRST_NAMES = ["Alice", "Bob", "Carol", "David"]
LAST_NAMES = ["Smith", "Johnson", "Lee", "Williams"]
COUNTRIES = ["USA", "UK", "Canada", "Germany"]
GENRES = ["Fiction", "Sci-Fi", "History", "Romance"]

ORDER_STATUSES = ['pending', 'shipped', 'delivered', 'cancelled']
SHIPPING_STATUSES = ['created', 'in_transit', 'delivered', 'delayed', 'returned', 'cancelled']


# --- Database Connections ---
def connect_postgres():
    return psycopg2.connect(**PG_CONFIG)


def connect_mysql():
    return mysql.connector.connect(**MYSQL_CONFIG)


# --- Core Logic ---
def insert_order(pg_conn, mysql_conn):
    with pg_conn.cursor() as pg_cur, mysql_conn.cursor() as my_cur:
        pg_cur.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
        customer = pg_cur.fetchone()
        if not customer:
            pg_cur.execute("""
                INSERT INTO customers (first_name, last_name, email, street_address, city, state, postal_code, country)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                random.choice(FIRST_NAMES), random.choice(LAST_NAMES),
                f"user{random.randint(1000,9999)}@example.com",
                "123 Main St", "Anytown", "CA", "12345", random.choice(COUNTRIES)
            ))

        pg_cur.execute("SELECT customer_id FROM customers ORDER BY RANDOM() LIMIT 1")
        customer_id = pg_cur.fetchone()[0]

        pg_cur.execute("""
            INSERT INTO orders (customer_id, status, shipping_method)
            VALUES (%s, %s, %s) RETURNING order_id
        """, (customer_id, 'pending', 'standard'))
        order_id = pg_cur.fetchone()[0]

        pg_cur.execute("SELECT book_id, price FROM books ORDER BY RANDOM() LIMIT 1")
        book = pg_cur.fetchone()
        if not book:
            pg_cur.execute("""
                INSERT INTO authors (first_name, last_name, country)
                VALUES (%s, %s, %s) RETURNING author_id
            """, (random.choice(FIRST_NAMES), random.choice(LAST_NAMES), random.choice(COUNTRIES)))
            author_id = pg_cur.fetchone()[0]
            pg_cur.execute("""
                INSERT INTO books (title, author_id, isbn, price, genre, stock)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                f"Book {random.randint(100,999)}", author_id,
                f"{random.randint(1000000000000,9999999999999)}",
                round(random.uniform(5.0, 50.0), 2),
                random.choice(GENRES), random.randint(5, 20)
            ))
            pg_cur.execute("SELECT book_id, price FROM books ORDER BY RANDOM() LIMIT 1")
            book = pg_cur.fetchone()

        book_id, price = book
        pg_cur.execute("""
            INSERT INTO order_items (order_id, book_id, quantity, price_at_purchase)
            VALUES (%s, %s, %s, %s)
        """, (order_id, book_id, random.randint(1, 3), price))

        my_cur.execute("SELECT carrier_id FROM carriers ORDER BY RAND() LIMIT 1")
        carrier = my_cur.fetchone()
        if not carrier:
            my_cur.execute("INSERT INTO carriers (name) VALUES (%s)", (f"Carrier {random.randint(1,100)}",))
            carrier_id = my_cur.lastrowid
        else:
            carrier_id = carrier[0]

        my_cur.execute("SELECT service_id FROM shipping_services WHERE carrier_id = %s ORDER BY RAND() LIMIT 1", (carrier_id,))
        service = my_cur.fetchone()
        if not service:
            my_cur.execute("""
                INSERT INTO shipping_services (carrier_id, service_name, estimated_days, cost_estimate)
                VALUES (%s, %s, %s, %s)
            """, (carrier_id, "Standard", 3, round(random.uniform(2.5, 15.0), 2)))
            service_id = my_cur.lastrowid
        else:
            service_id = service[0]

        tracking_number = f"TRK{random.randint(1000000, 9999999)}"
        my_cur.execute("""
            INSERT INTO shipments (order_id, carrier_id, service_id, tracking_number, shipping_status, shipped_date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (order_id, carrier_id, service_id, tracking_number, 'created', datetime.now()))

    pg_conn.commit()
    mysql_conn.commit()


def random_update_or_delete(pg_conn, mysql_conn):
    with pg_conn.cursor() as pg_cur:
        if random.random() < 0.7:
            pg_cur.execute("UPDATE orders SET status = %s WHERE order_id IN (SELECT order_id FROM orders ORDER BY RANDOM() LIMIT 1)",
                           (random.choice(ORDER_STATUSES),))
        else:
            pg_cur.execute("DELETE FROM order_items WHERE order_item_id IN (SELECT order_item_id FROM order_items ORDER BY RANDOM() LIMIT 1)")
    pg_conn.commit()


def main():
    pg_conn = connect_postgres()
    mysql_conn = connect_mysql()
    try:
        while True:
            if random.random() < 0.6:
                insert_order(pg_conn, mysql_conn)
            else:
                random_update_or_delete(pg_conn, mysql_conn)
            time.sleep(random.uniform(0.15, 0.25))
    finally:
        pg_conn.close()
        mysql_conn.close()


if __name__ == "__main__":
    main()
