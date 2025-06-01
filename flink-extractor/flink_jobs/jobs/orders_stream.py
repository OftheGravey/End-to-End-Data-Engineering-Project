from pyflink.table import EnvironmentSettings, TableEnvironment
from common.config import *  # noqa: F403

TOPIC = "pg-changes.public.orders"

def orders_stream(kafka_group: str):
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)

    table_env.execute_sql(f"""
        CREATE TABLE orders_source (
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
        CREATE TABLE orders_sink (
            order_id INTEGER,
            customer_id INTEGER NOT NULL,
            order_date TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            shipping_method VARCHAR(20),
            {POSTGRES_DEBEZIUM_METADATA_CREATE_TABLE}
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{DW_HOSTNAME}:{DW_PORT}/{DW_DATABASE}',
            'table-name' = '{DW_SCHEMA}.orders',
            'driver' = 'org.postgresql.Driver',
            'username' = '{DW_USERNAME}',
            'password' = '{DW_PASSWORD}'
        )
    """)

    table_env.execute_sql(f"""
        INSERT INTO orders_sink
        SELECT 
            CAST(JSON_VALUE(payload, '$.after.order_id') AS INTEGER) AS order_id,
            CAST(JSON_VALUE(payload, '$.after.customer_id') AS INTEGER) AS customer_id,
            CAST(FROM_UNIXTIME(CAST(JSON_VALUE(payload, '$.after.order_date') AS BIGINT)/1000000) AS TIMESTAMP) AS order_date,
            CAST(JSON_VALUE(payload, '$.after.status') AS VARCHAR(20)) AS status,
            CAST(JSON_VALUE(payload, '$.after.shipping_method') AS VARCHAR(20)) AS shipping_method,
            {POSTGRES_DEBEZIUM_METADATA_INSERT_INTO}
        FROM orders_source;
    """)


if __name__ == "__main__":
    orders_stream(kafka_group=STREAM_KAFKA_GROUP)
