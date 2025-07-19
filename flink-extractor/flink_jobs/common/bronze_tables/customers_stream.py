from common.bronze_tables.base_stream import KafkaDataStreamReader

TOPIC = "pg-changes.public.customers"

class CustomersDataStreamReader(KafkaDataStreamReader):
    def __init__(self, t_env, kafka_group, is_materialized):
        super().__init__(
            TOPIC, "customers", "postgres", t_env, kafka_group, is_materialized
        )

    def parse_kafka_create_rows(self):
        return """
            customer_id INTEGER,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            email VARCHAR(50),
            phone VARCHAR(16),
            created_at TIMESTAMP,
            street_address VARCHAR(50),
            city VARCHAR(50),
            state VARCHAR(50),
            postal_code VARCHAR(10),
            country VARCHAR(50)
        """
    
    def parse_kafka_insert_rows(self):
        return """
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
            CAST(JSON_VALUE(payload, '$.after.country') AS VARCHAR(50)) AS country
        """
  