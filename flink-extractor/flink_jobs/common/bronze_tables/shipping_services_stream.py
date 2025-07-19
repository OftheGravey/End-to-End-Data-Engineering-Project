from common.bronze_tables.base_stream import KafkaDataStreamReader

TOPIC = "mysql-changes.shipping_db.shipping_services"


class ShippingServicesDataStreamReader(KafkaDataStreamReader):
    def __init__(self, t_env, kafka_group, is_materialized):
        super().__init__(
            TOPIC, "shipping_services", "mysql", t_env, kafka_group, is_materialized
        )

    def parse_kafka_create_rows(self):
        return """
            service_id INTEGER,
            carrier_id INTEGER,
            service_name VARCHAR(255),
            estimated_days INTEGER,
            cost_estimate DECIMAL(10, 2)
        """

    def parse_kafka_insert_rows(self):
        return """
            CAST(JSON_VALUE(payload, '$.after.service_id') AS INTEGER) AS service_id,
            CAST(JSON_VALUE(payload, '$.after.carrier_id') AS INTEGER) AS carrier_id,
            CAST(JSON_VALUE(payload, '$.after.service_name') AS VARCHAR(255)) AS service_name,
            CAST(JSON_VALUE(payload, '$.after.estimated_days') AS INTEGER) AS estimated_days,
            base64_to_double(CAST(JSON_VALUE(payload, '$.after.cost_estimate') AS STRING), 2) AS cost_estimate
        """