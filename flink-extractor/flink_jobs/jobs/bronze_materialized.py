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

from pyflink.table import EnvironmentSettings, TableEnvironment
from common.functions import base64_to_double

KAFKA_GROUP_NAME = 'BRONZE_SINK'

if __name__ == '__main__':
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)
    
    table_env.create_temporary_function("base64_to_double", base64_to_double)
    args = {
        'table_env': table_env,
        'kafka_group': KAFKA_GROUP_NAME,
        'is_materialized': True
    }

    streams: list[KafkaDataStreamReader] = [
        BooksDataStreamReader(**args),
        OrderItemsDataStreamReader(**args),
        OrdersDataStreamReader(**args),
        AuthorsDataStreamReader(**args),
        CarriersDataStreamReader(**args),
        CustomersDataStreamReader(**args),
        ShipmentEventsDataStreamReader(**args),
        ShipmentsDataStreamReader(**args),
        ShippingServicesDataStreamReader(**args)
    ]

    for stream in streams:
        stream.create_topic_table()
        stream.create_formatted_table()
        stream.insert_into_landing_table()