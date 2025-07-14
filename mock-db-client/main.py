import random
import string
import time
from datetime import datetime, timedelta

from sqlalchemy import create_engine, MetaData, Table, insert, update, select, func
from sqlalchemy.exc import IntegrityError
from faker import Faker

fake = Faker()

# Database connections
store_engine = create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/store_db")
shipping_engine = create_engine("mysql+pymysql://root:root@localhost:3306/shipping_db")

store_metadata = MetaData()
store_metadata.reflect(store_engine)
store_tables = {t.name: t for t in store_metadata.tables.values()}

shipping_metadata = MetaData()
shipping_metadata.reflect(shipping_engine)
shipping_tables = {t.name: t for t in shipping_metadata.tables.values()}

# Constants
CARRIERS = [
    {"carrier_id": 1, "name": "FedEx", "contact_email": "support@fedex.com", "phone": "1234567890", "tracking_url_template": "http://fedex.com/track/{tracking_number}"},
    {"carrier_id": 2, "name": "UPS", "contact_email": "support@ups.com", "phone": "0987654321", "tracking_url_template": "http://ups.com/track/{tracking_number}"}
]

SERVICES = [
    {"service_id": 1, "carrier_id": 1, "service_name": "FedEx Ground", "estimated_days": 3, "cost_estimate": 10.00},
    {"service_id": 2, "carrier_id": 2, "service_name": "UPS 2nd Day Air", "estimated_days": 2, "cost_estimate": 15.00}
]

ORDER_STATUSES = ['pending', 'shipped', 'delivered', 'cancelled']
SHIPMENT_STATUSES = ['created', 'in_transit', 'delivered', 'delayed', 'returned', 'cancelled']

def init_shipping_constants():
    with shipping_engine.begin() as conn:
        for carrier in CARRIERS:
            conn.execute(insert(shipping_tables['carriers']).prefix_with("IGNORE"), carrier)
        for service in SERVICES:
            conn.execute(insert(shipping_tables['shipping_services']).prefix_with("IGNORE"), service)

def get_random_service():
    with shipping_engine.connect() as conn:
        services = conn.execute(select(shipping_tables['shipping_services'])).fetchall()
        return random.choice(services) if services else None

def insert_random_customer():
    customer = {
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.unique.email(),
        "phone": fake.phone_number(),
        "created_at": datetime.utcnow(),
        "street_address": fake.street_address(),
        "city": fake.city(),
        "state": fake.state(),
        "postal_code": fake.postcode(),
        "country": fake.country()
    }
    with store_engine.begin() as conn:
        result = conn.execute(insert(store_tables['customers']).returning(store_tables['customers'].c.customer_id), customer)
        customer_id = result.scalar()
    return customer_id

def insert_random_book():
    author = {
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "biography": fake.text(),
        "country": fake.country()
    }
    with store_engine.begin() as conn:
        res = conn.execute(insert(store_tables['authors']).returning(store_tables['authors'].c.author_id), author)
        author_id = res.scalar()

    book = {
        "title": fake.sentence(nb_words=3),
        "author_id": author_id,
        "isbn": fake.unique.isbn13(),
        "price": round(random.uniform(10.0, 100.0), 2),
        "published_date": fake.date_between(start_date='-5y', end_date='today'),
        "description": fake.text(max_nb_chars=64),  # <-- safely limited
        "genre": random.choice(["Fiction", "Non-Fiction", "Sci-Fi", "Fantasy"]),
        "stock": random.randint(10, 100)
    }
    with store_engine.begin() as conn:
        res = conn.execute(insert(store_tables['books']).returning(store_tables['books'].c.book_id), book)
        book_id = res.scalar()

    return book_id


def create_order_for_customer(customer_id):
    service = get_random_service()
    if not service:
        return

    order = {
        "customer_id": customer_id,
        "order_date": datetime.utcnow(),
        "status": "pending",
        "shipping_method": service.service_name
    }
    with store_engine.begin() as conn:
        result = conn.execute(insert(store_tables['orders']).returning(store_tables['orders'].c.order_id), order)
        order_id = result.scalar()

        # Insert multiple books per order
        for _ in range(random.randint(1, 3)):
            book_id = insert_random_book()
            conn.execute(insert(store_tables['order_items']), {
                "order_id": order_id,
                "book_id": book_id,
                "quantity": random.randint(1, 3),
                "price_at_purchase": round(random.uniform(10.0, 100.0), 2),
                "discount": round(random.uniform(0, 0.3), 2)
            })

    with shipping_engine.begin() as conn:
        conn.execute(insert(shipping_tables['shipments']), {
            "order_id": order_id,
            "carrier_id": service.carrier_id,
            "service_id": service.service_id,
            "tracking_number": ''.join(random.choices(string.ascii_uppercase + string.digits, k=10)),
            "shipping_status": "created",
            "shipped_date": datetime.utcnow().date(),
            "expected_delivery_date": (datetime.utcnow() + timedelta(days=service.estimated_days)).date(),
            "actual_delivery_date": None,
            "shipping_cost": service.cost_estimate
        })

def update_random_order_status():
    with store_engine.begin() as conn:
        order = conn.execute(select(store_tables['orders'].c.order_id).order_by(func.random()).limit(1)).scalar()
        if order:
            new_status = random.choice(ORDER_STATUSES)
            conn.execute(update(store_tables['orders']).where(store_tables['orders'].c.order_id == order).values(status=new_status))

def update_random_shipment_status():
    with shipping_engine.begin() as conn:
        shipment = conn.execute(select(shipping_tables['shipments'].c.shipment_id).order_by(func.rand()).limit(1)).scalar()
        if shipment:
            new_status = random.choice(SHIPMENT_STATUSES)
            conn.execute(update(shipping_tables['shipments']).where(shipping_tables['shipments'].c.shipment_id == shipment).values(shipping_status=new_status))

def main_loop():
    init_shipping_constants()
    customer_id = insert_random_customer()
    while True:
        action = random.choice(["order", "order", "update_order", "update_shipment"])
        if action == "order":
            create_order_for_customer(customer_id)
        elif action == "update_order":
            update_random_order_status()
        elif action == "update_shipment":
            update_random_shipment_status()


if __name__ == "__main__":
    main_loop()
