from common.bronze_tables.base_stream import KafkaDataStreamReader

TOPIC = "pg-changes.public.order_items"

class OrderItemsDataStreamReader(KafkaDataStreamReader):
    def __init__(self, t_env, kafka_group, is_materialized):
        super().__init__(
            TOPIC, "order_items", "postgres", t_env, kafka_group, is_materialized
        )

    def parse_kafka_create_rows(self):
        return """
            order_item_id INTEGER,
            order_id INTEGER NOT NULL,
            book_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            price_at_purchase DOUBLE PRECISION NOT NULL,
            discount DOUBLE PRECISION
        """
    
    def parse_kafka_insert_rows(self):
        return """
            CAST(JSON_VALUE(payload, '$.after.order_item_id') AS INTEGER) AS order_item_id,
            CAST(JSON_VALUE(payload, '$.after.order_id') AS INTEGER) AS order_id,
            CAST(JSON_VALUE(payload, '$.after.book_id') AS INTEGER) AS book_id,
            CAST(JSON_VALUE(payload, '$.after.quantity') AS INTEGER) AS quantity,
            base64_to_double(CAST(JSON_VALUE(payload, '$.after.price_at_purchase') AS STRING), 2) AS price_at_purchase,
            base64_to_double(CAST(JSON_VALUE(payload, '$.after.discount') AS STRING), 2) AS discount
        """
 