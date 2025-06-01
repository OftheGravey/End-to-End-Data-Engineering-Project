from pyflink.table import EnvironmentSettings, TableEnvironment
from common.config import *  # noqa: F403
from common.functions import base64_to_double

TOPIC = "pg-changes.public.order_items"

def order_items_stream(kafka_group: str):
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)

    table_env.execute_sql(f"""
        CREATE TABLE order_items_source (
            payload STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'properties.group.id' = '{kafka_group}',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        );
    """)

    table_env.execute_sql(f"""
       CREATE TABLE order_items_sink (
        order_item_id INTEGER,
        order_id INTEGER NOT NULL,
        book_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL,
        price_at_purchase DOUBLE PRECISION NOT NULL,
        discount DOUBLE PRECISION,
        {POSTGRES_DEBEZIUM_METADATA_CREATE_TABLE}
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://{DW_HOSTNAME}:{DW_PORT}/{DW_DATABASE}',
        'table-name' = '{DW_SCHEMA}.order_items',
        'driver' = 'org.postgresql.Driver',
        'username' = '{DW_USERNAME}',
        'password' = '{DW_PASSWORD}'
    );
    """)

    table_env.create_temporary_function('base64_to_double', base64_to_double)
    table_env.execute_sql(f"""
        INSERT INTO order_items_sink
        SELECT 
            CAST(JSON_VALUE(payload, '$.after.order_item_id') AS INTEGER) AS order_item_id,
            CAST(JSON_VALUE(payload, '$.after.order_id') AS INTEGER) AS order_id,
            CAST(JSON_VALUE(payload, '$.after.book_id') AS INTEGER) AS book_id,
            CAST(JSON_VALUE(payload, '$.after.quantity') AS INTEGER) AS quantity,
            base64_to_double(CAST(JSON_VALUE(payload, '$.after.price_at_purchase') AS STRING), 2) AS price_at_purchase,
            base64_to_double(CAST(JSON_VALUE(payload, '$.after.discount') AS STRING), 2) AS discount,
            {POSTGRES_DEBEZIUM_METADATA_INSERT_INTO}
        FROM order_items_source;
    """)


if __name__ == "__main__":
    order_items_stream(kafka_group=STREAM_KAFKA_GROUP)
