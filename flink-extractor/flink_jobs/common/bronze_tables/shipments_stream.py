from common.bronze_tables.base_stream import KafkaDataStreamReader

TOPIC = "mysql-changes.shipping_db.shipments"

class ShipmentsDataStreamReader(KafkaDataStreamReader):
    def __init__(self, table_env, kafka_group, is_materialized):
        super().__init__(
            TOPIC, "shipments", "mysql", table_env, kafka_group, is_materialized
        )

    def parse_kafka_create_rows(self):
        return """
            shipment_id INTEGER,
            order_id INTEGER,
            carrier_id INTEGER,
            service_id INTEGER,
            tracking_number STRING,
            shipping_status STRING,
            shipped_date DATE,
            expected_delivery_date DATE,
            actual_delivery_date DATE,
            shipping_cost DECIMAL(10, 2)
        """
    
    def parse_kafka_insert_rows(self):
        return """
            CAST(JSON_VALUE(payload, '$.after.shipment_id') AS INTEGER) AS shipment_id,
            CAST(JSON_VALUE(payload, '$.after.order_id') AS INTEGER) AS order_id,
            CAST(JSON_VALUE(payload, '$.after.carrier_id') AS INTEGER) AS carrier_id,
            CAST(JSON_VALUE(payload, '$.after.service_id') AS INTEGER) AS service_id,
            CAST(JSON_VALUE(payload, '$.after.tracking_number') AS STRING) AS tracking_number,
            CAST(JSON_VALUE(payload, '$.after.shipping_status') AS STRING) AS shipping_status,
            CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.shipped_date') AS INTEGER)) AS DATE) AS shipped_date,
            CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.expected_delivery_date') AS INTEGER)) AS DATE) AS expected_delivery_date,
            CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.actual_delivery_date') AS INTEGER)) AS DATE) AS actual_delivery_date,
            base64_to_double(CAST(JSON_VALUE(payload, '$.after.shipping_cost') AS STRING), 2) AS shipping_cost
        """
