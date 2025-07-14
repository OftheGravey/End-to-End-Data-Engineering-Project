from common.bronze_tables import (
    books_stream,
    order_items_stream,
    orders_stream,
    authors_stream,
    carriers_stream,
    customers_stream,
    shipment_events_stream,
    shipments_stream,
    shipping_services_stream
)
from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_GROUP_NAME = 'BRONZE_SINK'

if __name__ == '__main__':
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)
    
    # Register topics as tables
    books_stream.create_table(table_env, KAFKA_GROUP_NAME)
    order_items_stream.create_table(table_env, KAFKA_GROUP_NAME)
    orders_stream.create_table(table_env, KAFKA_GROUP_NAME)
    authors_stream.create_table(table_env, KAFKA_GROUP_NAME)
    carriers_stream.create_table(table_env, KAFKA_GROUP_NAME)
    customers_stream.create_table(table_env, KAFKA_GROUP_NAME)
    shipment_events_stream.create_table(table_env, KAFKA_GROUP_NAME)
    shipments_stream.create_table(table_env, KAFKA_GROUP_NAME)
    shipping_services_stream.create_table(table_env, KAFKA_GROUP_NAME)
