from common.bronze_tables.books_stream import BooksDataStreamReader
from common.bronze_tables.order_items_stream import OrderItemsDataStreamReader
from common.bronze_tables.orders_stream import OrdersDataStreamReader
from common.bronze_tables.authors_stream import AuthorsDataStreamReader
from common.bronze_tables.carriers_stream import CarriersDataStreamReader
from common.bronze_tables.customers_stream import CustomersDataStreamReader
from common.bronze_tables.shipment_events_stream import ShipmentEventsDataStreamReader
from common.bronze_tables.shipments_stream import ShipmentsDataStreamReader
from common.bronze_tables.shipping_services_stream import ShippingServicesDataStreamReader
from common.bronze_tables.base_stream import KafkaDataStreamReader
from common.silver_tables.base_sink import BaseDataSink

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment
from common.functions import base64_to_double, uuid_gen

KAFKA_GROUP_NAME = 'DEVELOPMENT'

if __name__ == '__main__':
    # 1. Create the low-level DataStream execution environment
    exec_env = StreamExecutionEnvironment.get_execution_environment()

    # 2. Create a StreamTableEnvironment *with* that execution environment
    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(exec_env, environment_settings=env_settings)
    
    t_env.create_temporary_function("base64_to_double", base64_to_double)
    t_env.create_temporary_function("uuid_gen", uuid_gen)
    args = {
        't_env': t_env,
        'kafka_group': KAFKA_GROUP_NAME,
        'is_materialized': False
    }

    streams: list[KafkaDataStreamReader] = [
        # BooksDataStreamReader(**args),
        # OrderItemsDataStreamReader(**args),
        OrdersDataStreamReader(**args),
        # AuthorsDataStreamReader(**args),
        # CarriersDataStreamReader(**args),
        # CustomersDataStreamReader(**args),
        # ShipmentEventsDataStreamReader(**args),
        # ShipmentsDataStreamReader(**args),
        # ShippingServicesDataStreamReader(**args)
    ]

    # Process stream into bronze tables
    for stream in streams:
        stream.create_topic_table()
        stream.create_formatted_table()
        stream.insert_into_landing_table()

    orders = BaseDataSink('d_orders', t_env)
    orders.insert_records()

    

    