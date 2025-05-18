import sys
import os
import pytest
import hashlib
import phonenumbers
import base64
from datetime import datetime, date
from dateutil.relativedelta import relativedelta

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from main import (
    op_hash,
    op_mask_email,
    op_mask_phone_number,
    process_dtypes,
    develop_insert_query,
)

def test_op_hash():
    value = "hello"
    expected = hashlib.sha256(value.encode()).hexdigest()
    assert op_hash(value) == expected


def test_op_hash_different_inputs():
    assert op_hash("abc") != op_hash("def")
    assert op_hash("") == hashlib.sha256("".encode()).hexdigest()


def test_op_mask_email_valid():
    assert op_mask_email("john.doe@example.com") == "jxxxxxxx@example.com"
    assert op_mask_email("a@b.com") == "a@b.com"
    assert op_mask_email("bob@domain.org") == "bxx@domain.org"


def test_op_mask_email_invalid():
    assert op_mask_email("missingatsign.com") is None
    assert op_mask_email("too@many@ats.com") is None
    assert op_mask_email("") is None


def test_op_mask_phone_number_valid():
    masked = op_mask_phone_number("+1 650-253-0000")  # Google HQ number
    assert masked.endswith("0000")
    assert masked.count("*") > 0
    assert len(masked) >= 10  # At least 10 characters


def test_op_mask_phone_number_invalid():
    assert op_mask_phone_number("not-a-number") is None
    assert op_mask_phone_number("") is None
    assert op_mask_phone_number("12345") is None  # Too short to be valid


def test_op_mask_phone_number_format():
    original = "+1 (202) 555-0191"
    masked = op_mask_phone_number(original)
    assert masked[-4:] == "0191"
    assert "*" in masked
    assert masked != original


def test_process_dtypes_string():
    col = {"type": "string"}
    assert process_dtypes(col, 123) == "123"
    assert process_dtypes(col, "abc") == "abc"


def test_process_dtypes_int():
    col = {"type": "int"}
    assert process_dtypes(col, "42") == 42
    assert process_dtypes(col, 10.0) == 10  # float to int


def test_process_dtypes_numeric():
    col = {"type": "numeric", "decimals": 2}
    value = 12345  # Means 123.45 after dividing by 10^2
    byte_value = value.to_bytes(
        (value.bit_length() + 7) // 8, byteorder="big", signed=True
    )
    encoded = base64.b64encode(byte_value)
    assert process_dtypes(col, encoded) == 123.45


def test_process_dtypes_numeric_negative():
    col = {"type": "numeric", "decimals": 2}
    value = -9876
    byte_value = value.to_bytes(
        (value.bit_length() + 8) // 8, byteorder="big", signed=True
    )
    encoded = base64.b64encode(byte_value).decode()
    assert process_dtypes(col, encoded) == -98.76


def test_process_dtypes_date():
    col = {"type": "date"}
    days_since_epoch = 1000
    expected = date(1970, 1, 1) + relativedelta(days=days_since_epoch)
    assert process_dtypes(col, days_since_epoch) == expected


def test_process_dtypes_timestamp():
    col = {"type": "timestamp"}
    ts_microseconds = 1_600_000_000_000_000  # Corresponds to a known datetime
    expected = datetime.fromtimestamp(ts_microseconds / 1_000_000)
    assert process_dtypes(col, ts_microseconds) == expected


def test_process_dtypes_none_value():
    col = {"type": "int"}
    assert process_dtypes(col, None) is None


def test_process_dtypes_invalid_type():
    col = {"type": "unsupported"}
    with pytest.raises(Exception, match="Invalid Column"):
        process_dtypes(col, "some value")


def test_process_dtypes_missing_decimals_in_numeric():
    col = {"type": "numeric"}  # Missing 'decimals' key
    process_dtypes(col, "ignored") is None


def test_develop_insert_query_basic():
    table = "users"
    record = {"id": 1, "name": "Alice", "email": "alice@example.com"}
    query = develop_insert_query(table, record)

    expected_query = """
    INSERT INTO users 
    (id, name, email)
    VALUES 
    (?,?,?)
    """
    assert query.strip() == expected_query.strip()


def test_develop_insert_query_single_column():
    table = "logs"
    record = {"timestamp": "2023-01-01 00:00:00"}
    query = develop_insert_query(table, record)

    expected_query = """
    INSERT INTO logs 
    (timestamp)
    VALUES 
    (?)
    """
    assert query.strip() == expected_query.strip()


def test_develop_insert_query_empty_record_raises():
    table = "empty_table"
    record = {}
    with pytest.raises(ValueError, match="Record must have at least one column"):
        develop_insert_query(table, record)


def test_develop_insert_query_special_column_names():
    table = "orders"
    record = {"order_id": 123, "customer_name": "John"}
    query = develop_insert_query(table, record)

    expected_query = """
    INSERT INTO orders 
    (order_id, customer_name)
    VALUES 
    (?,?)
    """
    assert query.strip() == expected_query.strip()


def test_develop_insert_query_column_order():
    table = "products"
    record = {"price": 9.99, "name": "Pen", "id": 101}
    query = develop_insert_query(table, record)

    expected_query = """
    INSERT INTO products 
    (price, name, id)
    VALUES 
    (?,?,?)
    """
    assert query.strip() == expected_query.strip()
