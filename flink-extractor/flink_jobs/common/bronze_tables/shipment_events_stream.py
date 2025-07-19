from common.bronze_tables.base_stream import KafkaDataStreamReader

TOPIC = "mysql-changes.shipping_db.shipment_events"

class ShipmentEventsDataStreamReader(KafkaDataStreamReader):
    def __init__(self, t_env, kafka_group, is_materialized):
        super().__init__(
            TOPIC, "shipment_events", "mysql", t_env, kafka_group, is_materialized
        )

    def parse_kafka_create_rows(self):
        return """
        event_id INTEGER,
        shipment_id INTEGER,
        status STRING,
        location STRING,
        event_time TIMESTAMP,
        notes STRING
        """
    
    def parse_kafka_insert_rows(self):
        return """
            CAST(JSON_VALUE(payload, '$.after.event_id') AS INTEGER) AS event_id,
            CAST(JSON_VALUE(payload, '$.after.shipment_id') AS INTEGER) AS shipment_id,
            CAST(JSON_VALUE(payload, '$.after.status') AS STRING) AS status,
            CAST(JSON_VALUE(payload, '$.after.location') AS STRING) AS location,
            CAST(JSON_VALUE(payload, '$.after.event_time') AS TIMESTAMP) AS event_time,
            CAST(JSON_VALUE(payload, '$.after.notes') AS STRING) AS notes
        """