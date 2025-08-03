from pyflink.table import EnvironmentSettings, TableEnvironment
from common.config import *  # noqa: F403
from common.functions import base64_to_double

TOPIC = "mysql-changes.shipping_db.shipping_services"

def shipping_services_stream(kafka_group: str):
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)

    table_env.execute_sql(f"""
        CREATE TABLE shipping_services_source (
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
       CREATE TABLE shipping_services_sink (
        service_id INTEGER,
        carrier_id INTEGER,
        service_name VARCHAR(255),
        estimated_days INTEGER,
        cost_estimate DECIMAL(10, 2),
        {MYSQL_DEBEZIUM_METADATA_CREATE_TABLE}
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://{DW_HOSTNAME}:{DW_PORT}/{DW_DATABASE}',
        'table-name' = '{DW_SCHEMA}.shipping_services',
        'driver' = 'org.postgresql.Driver',
        'username' = '{DW_USERNAME}',
        'password' = '{DW_PASSWORD}'
    );
    """)

    table_env.create_temporary_function('base64_to_double', base64_to_double)
    table_env.execute_sql(f"""
        INSERT INTO shipping_services_sink
        SELECT 
            CAST(JSON_VALUE(payload, '$.after.service_id') AS INTEGER) AS service_id,
            CAST(JSON_VALUE(payload, '$.after.carrier_id') AS INTEGER) AS carrier_id,
            CAST(JSON_VALUE(payload, '$.after.service_name') AS VARCHAR(255)) AS service_name,
            CAST(JSON_VALUE(payload, '$.after.estimated_days') AS INTEGER) AS estimated_days,
            base64_to_double(CAST(JSON_VALUE(payload, '$.after.cost_estimate') AS STRING), 2) AS cost_estimate,        
            {MYSQL_DEBEZIUM_METADATA_INSERT_INTO}
        FROM shipping_services_source;
    """)


if __name__ == "__main__":
    shipping_services_stream(kafka_group=STREAM_KAFKA_GROUP)
