from common.bronze_tables.base_stream import KafkaDataStreamReader

TOPIC = "pg-changes.public.orders"

class OrdersDataStreamReader(KafkaDataStreamReader):
    def __init__(self, table_env, kafka_group, is_materialized):
        super().__init__(
            TOPIC, "orders", "postgres", table_env, kafka_group, is_materialized
        )

    def parse_kafka_create_rows(self):
        return """
            order_id INTEGER,
            customer_id INTEGER NOT NULL,
            order_date TIMESTAMP,
            status VARCHAR(20) NOT NULL,
            shipping_method VARCHAR(20)
        """
    
    def parse_kafka_insert_rows(self):
        return """
            CAST(JSON_VALUE(payload, '$.after.order_id') AS INTEGER) AS order_id,
            CAST(JSON_VALUE(payload, '$.after.customer_id') AS INTEGER) AS customer_id,
            CAST(FROM_UNIXTIME(CAST(JSON_VALUE(payload, '$.after.order_date') AS BIGINT)/1000000) AS TIMESTAMP) AS order_date,
            CAST(JSON_VALUE(payload, '$.after.status') AS VARCHAR(20)) AS status,
            CAST(JSON_VALUE(payload, '$.after.shipping_method') AS VARCHAR(20)) AS shipping_method
        """