from pyflink.table import EnvironmentSettings, TableEnvironment
from common.config import *  # noqa: F403
from common.functions import base64_to_double

TOPIC = "pg-changes.public.books"

def books_stream(kafka_group: str):
    env_settings = EnvironmentSettings.in_streaming_mode()
    table_env = TableEnvironment.create(environment_settings=env_settings)

    table_env.execute_sql(f"""
        CREATE TABLE books_source (
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
        CREATE TABLE books_sink (
            book_id INTEGER,
            title VARCHAR(50),
            author_id INTEGER,
            isbn VARCHAR(50),
            price DECIMAL(10, 2),
            published_date DATE,
            description VARCHAR(500),
            genre VARCHAR(50),
            stock INTEGER,
            {POSTGRES_DEBEZIUM_METADATA_CREATE_TABLE}
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:postgresql://{DW_HOSTNAME}:{DW_PORT}/{DW_DATABASE}',
            'table-name' = '{DW_SCHEMA}.books',
            'driver' = 'org.postgresql.Driver',
            'username' = '{DW_USERNAME}',
            'password' = '{DW_PASSWORD}'
        )
    """)

    table_env.create_temporary_function('base64_to_double', base64_to_double)
    table_env.execute_sql(f"""
    INSERT INTO books_sink
    SELECT 
        CAST(JSON_VALUE(payload, '$.after.book_id') AS INTEGER) AS book_id,
        CAST(JSON_VALUE(payload, '$.after.title') AS VARCHAR(50)) AS title,
        CAST(JSON_VALUE(payload, '$.after.author_id') AS INTEGER) AS author_id,
        CAST(JSON_VALUE(payload, '$.after.isbn') AS VARCHAR(13)) AS isbn,
        base64_to_double(CAST(JSON_VALUE(payload, '$.after.price') AS STRING), 2) AS price,
        CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.published_date') AS INTEGER)) AS DATE) AS published_date,
        CAST(JSON_VALUE(payload, '$.after.description') AS VARCHAR(500)) AS description,
        CAST(JSON_VALUE(payload, '$.after.genre') AS VARCHAR(50)) AS genre,
        CAST(JSON_VALUE(payload, '$.after.stock') AS INTEGER) AS stock,
        {POSTGRES_DEBEZIUM_METADATA_INSERT_INTO}
    FROM books_source;
    """)


if __name__ == "__main__":
    books_stream(kafka_group=STREAM_KAFKA_GROUP)
