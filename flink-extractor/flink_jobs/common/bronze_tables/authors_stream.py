from common.bronze_tables.base_stream import KafkaDataStreamReader

TOPIC = "pg-changes.public.authors"

class AuthorsDataStreamReader(KafkaDataStreamReader):
    def __init__(self, t_env, kafka_group, is_materialized):
        super().__init__(
            TOPIC, "authors", "postgres", t_env, kafka_group, is_materialized
        )

    def parse_kafka_create_rows(self):
        return """
            author_id INTEGER,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            biography VARCHAR(50),
            country VARCHAR(50)
        """
    
    def parse_kafka_insert_rows(self):
        return """
            CAST(JSON_VALUE(payload, '$.after.author_id') AS INTEGER) AS author_id,
            CAST(JSON_VALUE(payload, '$.after.first_name') AS VARCHAR(50)) AS first_name,
            CAST(JSON_VALUE(payload, '$.after.last_name') AS VARCHAR(50)) AS last_name,
            CAST(JSON_VALUE(payload, '$.after.biography') AS VARCHAR(50)) AS biography,
            CAST(JSON_VALUE(payload, '$.after.country') AS VARCHAR(50)) AS country
        """