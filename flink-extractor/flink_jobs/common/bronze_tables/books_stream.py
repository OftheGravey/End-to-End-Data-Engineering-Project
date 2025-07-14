from common.bronze_tables.base_stream import KafkaDataStreamReader

TOPIC = "pg-changes.public.books"

class BooksDataStreamReader(KafkaDataStreamReader):
    def __init__(self, table_env, kafka_group, is_materialized):
        super().__init__(
            TOPIC, "books", "postgres", table_env, kafka_group, is_materialized
        )

    def parse_kafka_create_rows(self):
        return """
            book_id INTEGER,
            title VARCHAR(50),
            author_id INTEGER,
            isbn VARCHAR(50),
            price DECIMAL(10, 2),
            published_date DATE,
            description VARCHAR(500),
            genre VARCHAR(50),
            stock INTEGER
        """
    
    def parse_kafka_insert_rows(self):
        return """
            CAST(JSON_VALUE(payload, '$.after.book_id') AS INTEGER) AS book_id,
            CAST(JSON_VALUE(payload, '$.after.title') AS VARCHAR(50)) AS title,
            CAST(JSON_VALUE(payload, '$.after.author_id') AS INTEGER) AS author_id,
            CAST(JSON_VALUE(payload, '$.after.isbn') AS VARCHAR(13)) AS isbn,
            base64_to_double(CAST(JSON_VALUE(payload, '$.after.price') AS STRING), 2) AS price,
            CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.published_date') AS INTEGER)) AS DATE) AS published_date,
            CAST(JSON_VALUE(payload, '$.after.description') AS VARCHAR(500)) AS description,
            CAST(JSON_VALUE(payload, '$.after.genre') AS VARCHAR(50)) AS genre,
            CAST(JSON_VALUE(payload, '$.after.stock') AS INTEGER) AS stock
        """