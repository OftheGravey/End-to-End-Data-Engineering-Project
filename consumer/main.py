import duckdb
import json
import yaml
import re
import base64
import hashlib
import phonenumbers
from enum import Enum
from kafka import KafkaConsumer
from fastavro import schemaless_reader
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from threading import Thread
from functools import partial


def op_hash(value):
    return hashlib.sha256(value.encode()).hexdigest()

def op_mask_email(email: str):
    if email.count('@') != 1:
        # invalid email format
        return None
    # hereiam@gmail.com -> hxxxxxx@gmail.com
    name = email.split('@')[0]
    domain = email.split('@')[1]
    name = name[0] + ''.join(['x' for _ in name[1:]])
    return f"{name}@{domain}"

def op_mask_phone_number(phone_number: str):
    try:
        ph = phonenumbers.parse(phone_number, region="US")
        # if not phonenumbers.is_valid_number(ph):
        #     print("Invalid Number")
        #     return None
        formatted = phonenumbers.format_number(ph, phonenumbers.PhoneNumberFormat.NATIONAL)
        digits = [c if not c.isdigit() else '*' for c in formatted[:-4]]
        digits += formatted[-4:]  # Keep last 4 digits unmasked
        return ''.join(digits)
    except phonenumbers.NumberParseException as e:
        print(f"parsing error {e}")
        return None

class DataOperator(Enum):
    hash = partial(op_hash)
    mask_email = partial(op_mask_email)
    mask_phone_number = partial(op_mask_phone_number)

server_uri = "localhost:29092"

def load_schema(topic):
    with open("schemas/tables.yml") as file:
        schema = yaml.safe_load(file)
    schema = schema[topic]
    return schema

def process_dtypes(col, value):
    if value is None:
        return None
    if col['type'] == 'string':
        return str(value)
    elif col['type'] == 'int':
        return int(value)
    elif col['type'] == 'numeric':
        decoded_value = base64.b64decode(value)
        num_value = int.from_bytes(decoded_value, byteorder="big", signed=True)
        num_value /= 10 ** col['decimals']
        return num_value
    elif col['type'] == 'date':
        print(value)
        days = relativedelta(days=value)
        date_value = date(1970, 1, 1) + days
        return date_value
    elif col['type'] == 'timestamp':
        print(value)
        dt_value = datetime.fromtimestamp(value/1000000)
        return dt_value
    raise Exception("Invalid Column")

def parse_data(msg, schema, before=False):
    if msg.value is None:
        return
    data = json.loads(msg.value.decode('utf-8'))

    op = data['payload']['op']
    if op == 'd' or before:
        record = data['payload']['before']
    else:
        record = data['payload']['after']

    p_record = {}
    for col in record:
        if col not in schema['columns']:
            print(f"{col} not in schema. Implicitly omitting...")
            continue
        col_op = schema['columns'][col].get('op')
        process_col = process_dtypes(schema['columns'][col], record[col])
        if col_op is None:
            # Implicit pass through
            p_record[col] = process_col
            continue
        if col_op == 'omit':
            continue
        p_record[col] = DataOperator[col_op].value(process_col)

    p_record['op'] = op
    p_record['emitted_ts_ms'] = data['payload']['ts_ms']
    p_record['ts_ms'] = data['payload']['source']['ts_ms']
    p_record['connector_version'] = data['payload']['source']['version']

    if data['payload']['source']['connector'] == 'postgresql':
        p_record['lsn'] = data['payload']['source']['lsn']
        p_record['transaction_id'] = data['payload']['source']['txId']
    else:
        # currently shipping_db doesn't have gtid enabled. Will be null
        p_record['transaction_id'] = data['payload']['source']['gtid']

    # prune no changes update
    if op == 'u' and not before:
        old = parse_data(msg, schema, before=True)
        if old == p_record:
            print("HERE")
            return None 

    return p_record

def develop_insert_query(table, record):
    col_str = ', '.join([col for col in record.keys()])
    val_str = ','.join(['?' for _ in record.keys()])
    query = f"""
    INSERT INTO {table} 
    ({col_str})
    VALUES 
    ({val_str})
    """
    return query

def sub_to_topic(topic: str, conn: duckdb.DuckDBPyConnection):
    consumer = KafkaConsumer(
        f"{topic}",
        bootstrap_servers=[server_uri],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id="staging"
    )

    table_name = topic.split('.')[-1]
    schema = load_schema(table_name)
    for msg in consumer:
        record = parse_data(msg, schema)
        if record is None:
            continue
        insert_query = develop_insert_query(table_name, record)
        conn.execute(insert_query, tuple(record.values()))
        consumer.commit()

def init_staging_db(conn: duckdb.DuckDBPyConnection):
    with open("data/staging-init.sql") as init_sql:
        conn.execute(init_sql.read())

if __name__ == "__main__":
    with duckdb.connect("data/staging.db") as conn:
        init_staging_db(conn)

        #sub_to_topic('customers', conn)
        topics = [
            'pg-changes.public.authors',
            'pg-changes.public.orders',
            'pg-changes.public.order_items',
            'pg-changes.public.books',
            'pg-changes.public.customers',
            'mysql-changes.shipping_db.carriers',
            'mysql-changes.shipping_db.shipment_events',
            'mysql-changes.shipping_db.shipments',
            'mysql-changes.shipping_db.shipping_services'
        ]
        ts = [Thread(target=sub_to_topic, args=(tp, conn), daemon=True) for tp in topics]
        for t in ts:
            t.start()

        while 1:
            quit = input("Quit? ")
            if quit == 'q':
                break
            conn.commit()