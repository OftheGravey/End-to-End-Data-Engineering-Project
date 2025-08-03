import psycopg2
import mysql.connector
from faker import Faker
import random
import time
from datetime import datetime, timedelta

# --- Configuration ---
PG_DB_CONFIG = {
    'host': 'localhost',
    'database': 'store_db',
    'user': 'postgres',
    'password': 'postgres'
}

MYSQL_DB_CONFIG = {
    'host': 'localhost',
    'database': 'shipping_db',
    'user': 'root',
    'password': 'root'
}

# --- Global State for ID Management ---
# To ensure consistency across databases, we'll keep track of IDs created.
# In a real-world scenario with high concurrency, you'd need a more robust
# distributed ID generation strategy or rely on a CDC solution.
# For this mock client, we'll simulate this by sharing IDs.
# We'll also store some data to make "sensible" updates and related inserts.

authors_in_db = []  # Stores (author_id, first_name, last_name)
books_in_db = []    # Stores (book_id, author_id, price, stock)
customers_in_db = [] # Stores (customer_id, email)
orders_in_db = []   # Stores (order_id, customer_id, status)
carriers_in_db = [] # Stores (carrier_id, name)
shipping_services_in_db = [] # Stores (service_id, carrier_id)
shipments_in_db = [] # Stores (shipment_id, order_id, carrier_id, service_id, tracking_number, status)


fake = Faker()

# --- Database Connection Functions ---
def get_pg_connection():
    try:
        conn = psycopg2.connect(**PG_DB_CONFIG)
        return conn
    except psycopg2.Error as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def get_mysql_connection():
    try:
        conn = mysql.connector.connect(**MYSQL_DB_CONFIG)
        return conn
    except mysql.connector.Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

# --- Data Generation Functions ---
def generate_author_data():
    return {
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'biography': fake.text(),
        'country': fake.country()
    }

def generate_book_data(author_id):
    return {
        'title': fake.sentence(nb_words=random.randint(3, 7)).replace('.', ''),
        'author_id': author_id,
        'isbn': fake.isbn13(),
        'price': round(random.uniform(5.00, 100.00), 2),
        'published_date': fake.date_between(start_date='-50y', end_date='today'),
        'description': fake.paragraph(),
        'genre': fake.word(ext_word_list=['Fiction', 'Science Fiction', 'Fantasy', 'Mystery', 'Thriller', 'Romance', 'Biography', 'History']),
        'stock': random.randint(0, 500)
    }

def generate_customer_data():
    return {
        'first_name': fake.first_name(),
        'last_name': fake.last_name(),
        'email': fake.unique.email(),
        'phone': fake.phone_number(),
        'street_address': fake.street_address(),
        'city': fake.city(),
        'state': fake.state(),
        'postal_code': fake.postcode(),
        'country': fake.country()
    }

def generate_order_data(customer_id):
    return {
        'customer_id': customer_id,
        'status': random.choice(['pending', 'shipped', 'delivered', 'cancelled']),
        'shipping_method': random.choice(['Standard', 'Express', 'Priority']),
        'notes': fake.sentence() if random.random() < 0.5 else None
    }

def generate_order_item_data(order_id, book_id, price):
    quantity = random.randint(1, 5)
    discount = round(random.uniform(0, 0.2) * price, 2) if random.random() < 0.3 else 0.00
    return {
        'order_id': order_id,
        'book_id': book_id,
        'quantity': quantity,
        'price_at_purchase': price,
        'discount': discount,
        'comment': fake.sentence() if random.random() < 0.2 else None
    }

def generate_carrier_data():
    name = fake.company() + " Express"
    return {
        'name': name,
        'contact_email': fake.email(),
        'phone': fake.phone_number(),
        'tracking_url_template': f"https://{name.lower().replace(' ', '')}.com/track?num={{tracking_number}}"
    }

def generate_shipping_service_data(carrier_id):
    return {
        'carrier_id': carrier_id,
        'service_name': random.choice(['Standard Shipping', 'Expedited', 'Next Day', 'Economy']),
        'estimated_days': random.randint(1, 14),
        'cost_estimate': round(random.uniform(5.00, 50.00), 2)
    }

def generate_shipment_data(order_id, carrier_id, service_id):
    shipped_date = fake.date_this_year()
    expected_delivery_date = shipped_date + timedelta(days=random.randint(1, 14))
    shipping_status = random.choice(['created', 'in_transit', 'delivered', 'delayed', 'returned', 'cancelled'])
    actual_delivery_date = None
    if shipping_status == 'delivered':
        actual_delivery_date = expected_delivery_date + timedelta(days=random.randint(-2, 2))
    elif shipping_status == 'delayed':
        actual_delivery_date = expected_delivery_date + timedelta(days=random.randint(3, 7))

    return {
        'order_id': order_id,
        'carrier_id': carrier_id,
        'service_id': service_id,
        'tracking_number': fake.unique.bothify(text='############????##'),
        'shipping_status': shipping_status,
        'shipped_date': shipped_date,
        'expected_delivery_date': expected_delivery_date,
        'actual_delivery_date': actual_delivery_date,
        'shipping_cost': round(random.uniform(5.00, 100.00), 2)
    }

def generate_shipment_event_data(shipment_id, status):
    return {
        'shipment_id': shipment_id,
        'status': status,
        'location': fake.city() if random.random() < 0.7 else None,
        'event_time': fake.date_time_this_year(),
        'notes': fake.sentence() if random.random() < 0.3 else None
    }

# --- PostgreSQL Operations ---
def pg_insert_author(conn):
    data = generate_author_data()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO authors (first_name, last_name, biography, country) VALUES (%s, %s, %s, %s) RETURNING author_id",
                (data['first_name'], data['last_name'], data['biography'], data['country'])
            )
            author_id = cur.fetchone()[0]
            conn.commit()
            authors_in_db.append((author_id, data['first_name'], data['last_name']))
            print(f"PG Inserted Author: {author_id}")
            return author_id
    except psycopg2.Error as e:
        conn.rollback()
        print(f"PG Insert Author Error: {e}")
        return None

def pg_update_author(conn):
    if not authors_in_db:
        return
    author_id, _, _ = random.choice(authors_in_db)
    new_country = fake.country()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE authors SET country = %s WHERE author_id = %s",
                (new_country, author_id)
            )
            conn.commit()
            print(f"PG Updated Author {author_id}: New country {new_country}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"PG Update Author Error: {e}")

def pg_insert_book(conn):
    if not authors_in_db:
        print("No authors available to link books to. Skipping book insert.")
        return None
    author_id, _, _ = random.choice(authors_in_db)
    data = generate_book_data(author_id)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO books (title, author_id, isbn, price, published_date, description, genre, stock)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING book_id
                """,
                (data['title'], data['author_id'], data['isbn'], data['price'],
                 data['published_date'], data['description'], data['genre'], data['stock'])
            )
            book_id = cur.fetchone()[0]
            conn.commit()
            books_in_db.append((book_id, data['author_id'], data['price'], data['stock']))
            print(f"PG Inserted Book: {book_id} by Author {author_id}")
            return book_id
    except psycopg2.Error as e:
        conn.rollback()
        print(f"PG Insert Book Error: {e}")
        return None

def pg_update_book_stock(conn):
    if not books_in_db:
        return
    book_id, _, _, current_stock = random.choice(books_in_db)
    new_stock = random.randint(0, 500)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE books SET stock = %s WHERE book_id = %s",
                (new_stock, book_id)
            )
            conn.commit()
            # Update the in-memory stock for future reference
            for i, (bid, aid, price, stock) in enumerate(books_in_db):
                if bid == book_id:
                    books_in_db[i] = (bid, aid, price, new_stock)
                    break
            print(f"PG Updated Book {book_id}: New stock {new_stock}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"PG Update Book Stock Error: {e}")

def pg_insert_customer(conn):
    data = generate_customer_data()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO customers (first_name, last_name, email, phone, street_address, city, state, postal_code, country)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING customer_id
                """,
                (data['first_name'], data['last_name'], data['email'], data['phone'],
                 data['street_address'], data['city'], data['state'], data['postal_code'], data['country'])
            )
            customer_id = cur.fetchone()[0]
            conn.commit()
            customers_in_db.append((customer_id, data['email']))
            print(f"PG Inserted Customer: {customer_id}")
            return customer_id
    except psycopg2.Error as e:
        conn.rollback()
        print(f"PG Insert Customer Error: {e}")
        return None

def pg_update_customer_email(conn):
    if not customers_in_db:
        return
    customer_id, _ = random.choice(customers_in_db)
    new_email = fake.unique.email()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE customers SET email = %s WHERE customer_id = %s",
                (new_email, customer_id)
            )
            conn.commit()
            # Update the in-memory email
            for i, (cid, email) in enumerate(customers_in_db):
                if cid == customer_id:
                    customers_in_db[i] = (cid, new_email)
                    break
            print(f"PG Updated Customer {customer_id}: New email {new_email}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"PG Update Customer Email Error: {e}")

def pg_insert_order(conn):
    if not customers_in_db:
        print("No customers available to create orders. Skipping order insert.")
        return None
    customer_id, _ = random.choice(customers_in_db)
    data = generate_order_data(customer_id)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO orders (customer_id, status, shipping_method, notes) VALUES (%s, %s, %s, %s) RETURNING order_id",
                (data['customer_id'], data['status'], data['shipping_method'], data['notes'])
            )
            order_id = cur.fetchone()[0]
            conn.commit()
            orders_in_db.append((order_id, data['customer_id'], data['status']))
            print(f"PG Inserted Order: {order_id} for Customer {customer_id}")
            return order_id
    except psycopg2.Error as e:
        conn.rollback()
        print(f"PG Insert Order Error: {e}")
        return None

def pg_update_order_status(conn):
    if not orders_in_db:
        return
    order_id, _, _ = random.choice(orders_in_db)
    new_status = random.choice(['shipped', 'delivered', 'cancelled']) # Can't go back to pending
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE orders SET status = %s WHERE order_id = %s",
                (new_status, order_id)
            )
            conn.commit()
            # Update the in-memory status
            for i, (oid, cid, status) in enumerate(orders_in_db):
                if oid == order_id:
                    orders_in_db[i] = (oid, cid, new_status)
                    break
            print(f"PG Updated Order {order_id}: New status {new_status}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"PG Update Order Status Error: {e}")

def pg_insert_order_item(conn):
    if not orders_in_db or not books_in_db:
        print("No orders or books available to create order items. Skipping.")
        return
    order_id, _, _ = random.choice(orders_in_db)
    book_id, _, book_price, _ = random.choice(books_in_db)
    data = generate_order_item_data(order_id, book_id, book_price)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO order_items (order_id, book_id, quantity, price_at_purchase, discount, comment)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (data['order_id'], data['book_id'], data['quantity'],
                 data['price_at_purchase'], data['discount'], data['comment'])
            )
            conn.commit()
            print(f"PG Inserted Order Item: Order {order_id}, Book {book_id}")
    except psycopg2.Error as e:
        conn.rollback()
        print(f"PG Insert Order Item Error: {e}")

# --- MySQL Operations ---
def mysql_insert_carrier(conn):
    data = generate_carrier_data()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO carriers (name, contact_email, phone, tracking_url_template) VALUES (%s, %s, %s, %s)",
                (data['name'], data['contact_email'], data['phone'], data['tracking_url_template'])
            )
            carrier_id = cur.lastrowid
            conn.commit()
            carriers_in_db.append((carrier_id, data['name']))
            print(f"MySQL Inserted Carrier: {carrier_id}")
            return carrier_id
    except mysql.connector.Error as e:
        conn.rollback()
        print(f"MySQL Insert Carrier Error: {e}")
        return None

def mysql_update_carrier_email(conn):
    if not carriers_in_db:
        return
    carrier_id, _ = random.choice(carriers_in_db)
    new_email = fake.email()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE carriers SET contact_email = %s WHERE carrier_id = %s",
                (new_email, carrier_id)
            )
            conn.commit()
            print(f"MySQL Updated Carrier {carrier_id}: New email {new_email}")
    except mysql.connector.Error as e:
        conn.rollback()
        print(f"MySQL Update Carrier Email Error: {e}")

def mysql_insert_shipping_service(conn):
    if not carriers_in_db:
        print("No carriers available to create shipping services. Skipping.")
        return None
    carrier_id, _ = random.choice(carriers_in_db)
    data = generate_shipping_service_data(carrier_id)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO shipping_services (carrier_id, service_name, estimated_days, cost_estimate) VALUES (%s, %s, %s, %s)",
                (data['carrier_id'], data['service_name'], data['estimated_days'], data['cost_estimate'])
            )
            service_id = cur.lastrowid
            conn.commit()
            shipping_services_in_db.append((service_id, data['carrier_id']))
            print(f"MySQL Inserted Shipping Service: {service_id} for Carrier {carrier_id}")
            return service_id
    except mysql.connector.Error as e:
        conn.rollback()
        print(f"MySQL Insert Shipping Service Error: {e}")
        return None

def mysql_update_shipping_service_cost(conn):
    if not shipping_services_in_db:
        return
    service_id, _ = random.choice(shipping_services_in_db)
    new_cost = round(random.uniform(5.00, 50.00), 2)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE shipping_services SET cost_estimate = %s WHERE service_id = %s",
                (new_cost, service_id)
            )
            conn.commit()
            print(f"MySQL Updated Shipping Service {service_id}: New cost {new_cost}")
    except mysql.connector.Error as e:
        conn.rollback()
        print(f"MySQL Update Shipping Service Cost Error: {e}")

def mysql_insert_shipment(conn):
    if not orders_in_db or not carriers_in_db or not shipping_services_in_db:
        print("Not enough data to create a shipment (orders, carriers, or shipping services missing). Skipping.")
        return None
    order_id, _, _ = random.choice(orders_in_db)
    carrier_id, _ = random.choice(carriers_in_db)
    # Ensure a service exists for the chosen carrier, or pick any service
    available_services = [s for s in shipping_services_in_db if s[1] == carrier_id]
    if not available_services:
        # If no service for this carrier, just pick any available service
        if shipping_services_in_db:
            service_id, _ = random.choice(shipping_services_in_db)
        else:
            print("No shipping services available. Skipping shipment insert.")
            return None
    else:
        service_id, _ = random.choice(available_services)

    data = generate_shipment_data(order_id, carrier_id, service_id)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO shipments (order_id, carrier_id, service_id, tracking_number, shipping_status,
                shipped_date, expected_delivery_date, actual_delivery_date, shipping_cost)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (data['order_id'], data['carrier_id'], data['service_id'], data['tracking_number'],
                 data['shipping_status'], data['shipped_date'], data['expected_delivery_date'],
                 data['actual_delivery_date'], data['shipping_cost'])
            )
            shipment_id = cur.lastrowid
            conn.commit()
            shipments_in_db.append((shipment_id, data['order_id'], data['carrier_id'], data['service_id'], data['tracking_number'], data['shipping_status']))
            print(f"MySQL Inserted Shipment: {shipment_id} for Order {order_id}")
            # Also add an initial event for 'created' status
            mysql_insert_shipment_event(conn, shipment_id, 'created')
            return shipment_id
    except mysql.connector.Error as e:
        conn.rollback()
        print(f"MySQL Insert Shipment Error: {e}")
        return None

def mysql_update_shipment_status(conn):
    if not shipments_in_db:
        return
    shipment_id, _, _, _, _, current_status = random.choice(shipments_in_db)
    possible_next_statuses = {
        'created': ['in_transit', 'cancelled'],
        'in_transit': ['delivered', 'delayed', 'returned', 'cancelled'],
        'delivered': [],
        'delayed': ['in_transit', 'delivered', 'returned', 'cancelled'],
        'returned': [],
        'cancelled': []
    }
    next_statuses = possible_next_statuses.get(current_status, [])
    if not next_statuses:
        print(f"Shipment {shipment_id} is in a terminal status ({current_status}). Skipping update.")
        return

    new_status = random.choice(next_statuses)
    actual_delivery_date = None
    if new_status == 'delivered':
        actual_delivery_date = datetime.now().date() # Set to current date if delivered

    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE shipments SET shipping_status = %s, actual_delivery_date = %s WHERE shipment_id = %s",
                (new_status, actual_delivery_date, shipment_id)
            )
            conn.commit()
            # Update the in-memory status
            for i, (sid, oid, caid, svid, trk, status) in enumerate(shipments_in_db):
                if sid == shipment_id:
                    shipments_in_db[i] = (sid, oid, caid, svid, trk, new_status)
                    break
            print(f"MySQL Updated Shipment {shipment_id}: New status {new_status}")
            mysql_insert_shipment_event(conn, shipment_id, new_status) # Log the status change
    except mysql.connector.Error as e:
        conn.rollback()
        print(f"MySQL Update Shipment Status Error: {e}")

def mysql_insert_shipment_event(conn, shipment_id, status):
    data = generate_shipment_event_data(shipment_id, status)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO shipment_events (shipment_id, status, location, event_time, notes) VALUES (%s, %s, %s, %s, %s)",
                (data['shipment_id'], data['status'], data['location'], data['event_time'], data['notes'])
            )
            conn.commit()
            print(f"MySQL Inserted Shipment Event for Shipment {shipment_id}: Status {status}")
    except mysql.connector.Error as e:
        conn.rollback()
        print(f"MySQL Insert Shipment Event Error: {e}")

# --- Main Simulation Loop ---
def run_mock_client():
    pg_conn = None
    mysql_conn = None

    while True:
        try:
            if not pg_conn or pg_conn.closed:
                pg_conn = get_pg_connection()
                if pg_conn:
                    print("PostgreSQL connection re-established.")
            if not mysql_conn or mysql_conn.closed:
                mysql_conn = get_mysql_connection()
                if mysql_conn:
                    print("MySQL connection re-established.")

            if not pg_conn or not mysql_conn:
                print("Skipping operations due to missing database connection(s). Retrying in 5 seconds.")
                time.sleep(5)
                continue

            # Prioritize inserts to populate databases
            action_type = random.choices(['insert', 'update'], weights=[0.7, 0.3], k=1)[0]

            if action_type == 'insert':
                # PostgreSQL Inserts
                if random.random() < 0.2: # 20% chance to insert an author
                    pg_insert_author(pg_conn)
                elif random.random() < 0.3 and authors_in_db: # 30% chance to insert a book if authors exist
                    pg_insert_book(pg_conn)
                elif random.random() < 0.2: # 20% chance to insert a customer
                    pg_insert_customer(pg_conn)
                elif random.random() < 0.2 and customers_in_db: # 20% chance to insert an order if customers exist
                    order_id = pg_insert_order(pg_conn)
                    # If an order is created, there's a good chance to add order items to it
                    if order_id and books_in_db and random.random() < 0.7:
                        num_items = random.randint(1, 3)
                        for _ in range(num_items):
                            pg_insert_order_item(pg_conn)
                else: # Fallback for other inserts
                    if random.random() < 0.5:
                        pg_insert_order_item(pg_conn)

                # MySQL Inserts
                if random.random() < 0.2: # 20% chance to insert a carrier
                    mysql_insert_carrier(mysql_conn)
                elif random.random() < 0.3 and carriers_in_db: # 30% chance to insert a shipping service if carriers exist
                    mysql_insert_shipping_service(mysql_conn)
                elif random.random() < 0.4 and orders_in_db and carriers_in_db and shipping_services_in_db: # 40% chance to insert a shipment if dependencies exist
                    mysql_insert_shipment(mysql_conn)

            elif action_type == 'update':
                # PostgreSQL Updates
                update_pg_target = random.choice(['author', 'book_stock', 'customer_email', 'order_status'])
                if update_pg_target == 'author':
                    pg_update_author(pg_conn)
                elif update_pg_target == 'book_stock':
                    pg_update_book_stock(pg_conn)
                elif update_pg_target == 'customer_email':
                    pg_update_customer_email(pg_conn)
                elif update_pg_target == 'order_status':
                    pg_update_order_status(pg_conn)

                # MySQL Updates
                update_mysql_target = random.choice(['carrier_email', 'shipping_service_cost', 'shipment_status'])
                if update_mysql_target == 'carrier_email':
                    mysql_update_carrier_email(mysql_conn)
                elif update_mysql_target == 'shipping_service_cost':
                    mysql_update_shipping_service_cost(mysql_conn)
                elif update_mysql_target == 'shipment_status':
                    mysql_update_shipment_status(mysql_conn)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            # Attempt to close and re-open connections on major errors
            if pg_conn:
                pg_conn.close()
            if mysql_conn:
                mysql_conn.close()
            pg_conn = None
            mysql_conn = None

        #time.sleep(random.uniform(0.05, 0.1)) # Semi-random delay between operations

# --- Entry Point ---
if __name__ == "__main__":
    print("Starting mock client for PostgreSQL and MySQL databases...")
    print("Ensure both databases are running and accessible with the provided credentials.")
    print("The script will run indefinitely, performing semi-random inserts and updates.")
    run_mock_client()