from pyflink.table import EnvironmentSettings, TableEnvironment
from common.config import *  # noqa: F403

TOPIC = "mysql-changes.shipping_db.carriers"

def carriers_stream(kafka_group: str):
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)

    table_env.execute_sql(f"""
        CREATE TABLE carriers_source (
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
       CREATE TABLE carriers_sink (
        carrier_id INTEGER,
        name STRING,
        contact_email STRING,
        phone STRING,
        tracking_url_template STRING,
        {MYSQL_DEBEZIUM_METADATA_CREATE_TABLE}
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://{DW_HOSTNAME}:{DW_PORT}/{DW_DATABASE}',
        'table-name' = '{DW_SCHEMA}.carriers',
        'driver' = 'org.postgresql.Driver',
        'username' = '{DW_USERNAME}',
        'password' = '{DW_PASSWORD}'
    );
    """)

    table_env.execute_sql(f"""
        INSERT INTO carriers_sink
        SELECT 
            CAST(JSON_VALUE(payload, '$.after.carrier_id') AS INTEGER) AS carrier_id,
            CAST(JSON_VALUE(payload, '$.after.name') AS STRING) AS name,
            CAST(JSON_VALUE(payload, '$.after.contact_email') AS STRING) AS contact_email,
            CAST(JSON_VALUE(payload, '$.after.phone') AS STRING) AS phone,
            CAST(JSON_VALUE(payload, '$.after.tracking_url_template') AS STRING) AS tracking_url_template,
            {MYSQL_DEBEZIUM_METADATA_INSERT_INTO}
        FROM carriers_source;
    """)


if __name__ == "__main__":
    carriers_stream(kafka_group=STREAM_KAFKA_GROUP)
