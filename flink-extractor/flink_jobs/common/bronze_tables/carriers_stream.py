from common.bronze_tables.base_stream import KafkaDataStreamReader

TOPIC = "mysql-changes.shipping_db.carriers"

class CarriersDataStreamReader(KafkaDataStreamReader):
    def __init__(self, table_env, kafka_group, is_materialized):
        super().__init__(
            TOPIC, "carriers", "mysql", table_env, kafka_group, is_materialized
        )

    def parse_kafka_create_rows(self):
        return """
        carrier_id INTEGER,
        name STRING,
        contact_email STRING,
        phone STRING,
        tracking_url_template STRING
        """
    
    def parse_kafka_insert_rows(self):
        return """
            CAST(JSON_VALUE(payload, '$.after.carrier_id') AS INTEGER) AS carrier_id,
            CAST(JSON_VALUE(payload, '$.after.name') AS STRING) AS name,
            CAST(JSON_VALUE(payload, '$.after.contact_email') AS STRING) AS contact_email,
            CAST(JSON_VALUE(payload, '$.after.phone') AS STRING) AS phone,
            CAST(JSON_VALUE(payload, '$.after.tracking_url_template') AS STRING) AS tracking_url_template
        """