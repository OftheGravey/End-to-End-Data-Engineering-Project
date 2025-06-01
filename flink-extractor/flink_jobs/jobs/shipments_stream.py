from pyflink.table import EnvironmentSettings, TableEnvironment
from common.config import *  # noqa: F403
from common.functions import base64_to_double

TOPIC = "mysql-changes.shipping_db.shipments"

def shipments_stream(kafka_group: str):
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)

    table_env.execute_sql(f"""
        CREATE TABLE shipments_source (
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
       CREATE TABLE shipments_sink (
        shipment_id INTEGER,
        order_id INTEGER,
        carrier_id INTEGER,
        service_id INTEGER,
        tracking_number STRING,
        shipping_status STRING,
        shipped_date DATE,
        expected_delivery_date DATE,
        actual_delivery_date DATE,
        shipping_cost DECIMAL(10, 2),
        {MYSQL_DEBEZIUM_METADATA_CREATE_TABLE}
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://{DW_HOSTNAME}:{DW_PORT}/{DW_DATABASE}',
        'table-name' = '{DW_SCHEMA}.shipments',
        'driver' = 'org.postgresql.Driver',
        'username' = '{DW_USERNAME}',
        'password' = '{DW_PASSWORD}'
    );
    """)

    table_env.create_temporary_function('base64_to_double', base64_to_double)
    table_env.execute_sql(f"""
        INSERT INTO shipments_sink
        SELECT 
            CAST(JSON_VALUE(payload, '$.after.shipment_id') AS INTEGER) AS shipment_id,
            CAST(JSON_VALUE(payload, '$.after.order_id') AS INTEGER) AS order_id,
            CAST(JSON_VALUE(payload, '$.after.carrier_id') AS INTEGER) AS carrier_id,
            CAST(JSON_VALUE(payload, '$.after.service_id') AS INTEGER) AS service_id,
            CAST(JSON_VALUE(payload, '$.after.tracking_number') AS STRING) AS tracking_number,
            CAST(JSON_VALUE(payload, '$.after.shipping_status') AS STRING) AS shipping_status,
            CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.shipped_date') AS INTEGER)) AS DATE) AS shipped_date,
            CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.expected_delivery_date') AS INTEGER)) AS DATE) AS expected_delivery_date,
            CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.actual_delivery_date') AS INTEGER)) AS DATE) AS actual_delivery_date,
            base64_to_double(CAST(JSON_VALUE(payload, '$.after.shipping_cost') AS STRING), 2) AS shipping_cost,        
            {MYSQL_DEBEZIUM_METADATA_INSERT_INTO}
        FROM shipments_source;
    """)


if __name__ == "__main__":
    shipments_stream(kafka_group=STREAM_KAFKA_GROUP)
