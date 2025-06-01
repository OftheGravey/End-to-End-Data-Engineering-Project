from pyflink.table import EnvironmentSettings, TableEnvironment
from common.config import *  # noqa: F403

TOPIC = "pg-changes.public.customers"

def customers_stream(kafka_group: str):
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)

    table_env.execute_sql(f"""
        CREATE TABLE customers_source (
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
        CREATE TABLE customers_sink (
            customer_id INTEGER,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(50),
            phone VARCHAR(20),
            created_at TIMESTAMP,
            street_address VARCHAR(50),
            city VARCHAR(50),
            state VARCHAR(50),
            postal_code VARCHAR(10),
            country VARCHAR(50),
            {POSTGRES_DEBEZIUM_METADATA_CREATE_TABLE}
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{DW_HOSTNAME}:{DW_PORT}/{DW_DATABASE}',
            'table-name' = '{DW_SCHEMA}.customers',
            'driver' = 'org.postgresql.Driver',
            'username' = '{DW_USERNAME}',
            'password' = '{DW_PASSWORD}'
        );
    """)

    table_env.execute_sql(f"""
        INSERT INTO customers_sink
        SELECT 
            CAST(JSON_VALUE(payload, '$.after.customer_id') AS INTEGER) AS customer_id,
            CAST(JSON_VALUE(payload, '$.after.first_name') AS VARCHAR(50)) AS first_name,
            CAST(JSON_VALUE(payload, '$.after.last_name') AS VARCHAR(50)) AS last_name,
            CAST(JSON_VALUE(payload, '$.after.email') AS VARCHAR(50)) AS email,
            CAST(JSON_VALUE(payload, '$.after.phone') AS VARCHAR(20)) AS phone,
            CAST(FROM_UNIXTIME(CAST(JSON_VALUE(payload, '$.after.created_at') AS BIGINT)/1000000) AS TIMESTAMP) AS created_at,
            CAST(JSON_VALUE(payload, '$.after.street_address') AS VARCHAR(50)) AS street_address,
            CAST(JSON_VALUE(payload, '$.after.city') AS VARCHAR(50)) AS city,
            CAST(JSON_VALUE(payload, '$.after.state') AS VARCHAR(50)) AS state,
            CAST(JSON_VALUE(payload, '$.after.postal_code') AS VARCHAR(10)) AS postal_code,
            CAST(JSON_VALUE(payload, '$.after.country') AS VARCHAR(50)) AS country,
            {POSTGRES_DEBEZIUM_METADATA_INSERT_INTO}
        FROM customers_source;
    """)


if __name__ == "__main__":
    customers_stream(kafka_group=STREAM_KAFKA_GROUP)
