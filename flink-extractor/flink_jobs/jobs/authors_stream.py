from pyflink.table import EnvironmentSettings, TableEnvironment
from common.config import *  # noqa: F403

TOPIC = "pg-changes.public.authors"

def authors_stream(kafka_group: str):
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)

    table_env.execute_sql(f"""
        CREATE TABLE authors_source (
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
        CREATE TABLE authors_sink (
            author_id INTEGER,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            biography VARCHAR(50),
            country VARCHAR(50),
            {POSTGRES_DEBEZIUM_METADATA_CREATE_TABLE}
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{DW_HOSTNAME}:{DW_PORT}/{DW_DATABASE}',
            'table-name' = '{DW_SCHEMA}.authors',
            'driver' = 'org.postgresql.Driver',
            'username' = '{DW_USERNAME}',
            'password' = '{DW_PASSWORD}'
        )
    """)

    table_env.execute_sql(f"""
        INSERT INTO authors_sink
        SELECT 
            CAST(JSON_VALUE(payload, '$.after.author_id') AS INTEGER) AS author_id,
            CAST(JSON_VALUE(payload, '$.after.first_name') AS VARCHAR(50)) AS first_name,
            CAST(JSON_VALUE(payload, '$.after.last_name') AS VARCHAR(50)) AS last_name,
            CAST(JSON_VALUE(payload, '$.after.biography') AS VARCHAR(50)) AS biography,
            CAST(JSON_VALUE(payload, '$.after.country') AS VARCHAR(50)) AS country,
            {POSTGRES_DEBEZIUM_METADATA_INSERT_INTO}
        FROM authors_source;
    """)


if __name__ == "__main__":
    authors_stream(kafka_group=STREAM_KAFKA_GROUP)
