from pyflink.table import EnvironmentSettings, TableEnvironment
from common.config import *  # noqa: F403
from common.functions import base64_to_double

TOPIC = "mysql-changes.shipping_db.shipment_events"

def shipment_events_stream(kafka_group: str):
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)

    table_env.execute_sql(f"""
        CREATE TABLE shipment_events_source (
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
       CREATE TABLE shipment_events_sink (
        event_id INTEGER,
        shipment_id INTEGER,
        status STRING,
        location STRING,
        event_time TIMESTAMP,
        notes STRING,
        {MYSQL_DEBEZIUM_METADATA_CREATE_TABLE}
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://{DW_HOSTNAME}:{DW_PORT}/{DW_DATABASE}',
        'table-name' = '{DW_SCHEMA}.shipment_events',
        'driver' = 'org.postgresql.Driver',
        'username' = '{DW_USERNAME}',
        'password' = '{DW_PASSWORD}'
    );
    """)

    table_env.create_temporary_function('base64_to_double', base64_to_double)
    table_env.execute_sql(f"""
        INSERT INTO shipment_events_sink
        SELECT 
            CAST(JSON_VALUE(payload, '$.after.event_id') AS INTEGER) AS event_id,
            CAST(JSON_VALUE(payload, '$.after.shipment_id') AS INTEGER) AS shipment_id,
            CAST(JSON_VALUE(payload, '$.after.status') AS STRING) AS status,
            CAST(JSON_VALUE(payload, '$.after.location') AS STRING) AS location,
            CAST(JSON_VALUE(payload, '$.after.event_time') AS TIMESTAMP) AS event_time,
            CAST(JSON_VALUE(payload, '$.after.notes') AS STRING) AS notes,
            {MYSQL_DEBEZIUM_METADATA_INSERT_INTO}
        FROM shipment_events_source;
    """)


if __name__ == "__main__":
    shipment_events_stream(kafka_group=STREAM_KAFKA_GROUP)
