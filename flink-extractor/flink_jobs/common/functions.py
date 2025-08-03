from pyflink.table.udf import udf
from pyflink.table.types import DataTypes
import base64
import struct


@udf(result_type=DataTypes.DOUBLE())
def base64_to_double(value: str, decimals: int) -> float:
    decoded_value = base64.b64decode(value)
    num_value = int.from_bytes(decoded_value, byteorder="big", signed=True)
    num_value /= 10**decimals
    return num_value
