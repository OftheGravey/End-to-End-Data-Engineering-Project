package com.extractor.flink.jobs.landing;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.MySQLDebeziumColumns;
import com.extractor.flink.utils.TopicNameBuilder;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ShipmentsLandingJob {

    static String sourceTopic = "mysql-changes.shipping_db.shipments";
    public static String sinkTopic = TopicNameBuilder.build("shipping.shipments.landing");
    static String groupId = System.getenv("GROUP_ID");

    public static void main(String[] args) throws Exception {

        // Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceProperties = KafkaProperties.build(sourceTopic, groupId);
        String createTableDebeziumColumns = MySQLDebeziumColumns.createTableFormat();

        String createSourceTableSql = String.format("""
                CREATE TABLE shipments (
                    payload STRING
                ) %s
                """, sourceProperties);
        tableEnv.executeSql(createSourceTableSql);

        String sinkProperties = KafkaProperties.build(sinkTopic, groupId);

        String createSinkTableSql = String.format("""
                CREATE TABLE shipments_processed (
                    shipment_id INTEGER,
                    order_id INTEGER,
                    carrier_id INTEGER,
                    service_id INTEGER,
                    tracking_number STRING,
                    shipping_status STRING,
                    shipped_date DATE,
                    expected_delivery_date DATE,
                    actual_delivery_date DATE,
                    shipping_cost STRING,
                    %s
                ) %s
                """, createTableDebeziumColumns, sinkProperties);
        tableEnv.executeSql(createSinkTableSql);

        String insertTableDebeziumColumns = MySQLDebeziumColumns.insertTableFormat();
        String insertSinkSql = String.format(
                """
                        INSERT INTO shipments_processed
                        SELECT
                            CAST(JSON_VALUE(payload, '$.after.shipment_id') AS INTEGER) AS shipment_id,
                            CAST(JSON_VALUE(payload, '$.after.order_id') AS INTEGER) AS order_id,
                            CAST(JSON_VALUE(payload, '$.after.carrier_id') AS INTEGER) AS carrier_id,
                            CAST(JSON_VALUE(payload, '$.after.service_id') AS INTEGER) AS service_id,
                            CAST(JSON_VALUE(payload, '$.after.tracking_number') AS STRING) AS tracking_number,
                            CAST(JSON_VALUE(payload, '$.after.shipping_status') AS STRING) AS shipping_status,
                            CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.shipped_date') AS INTEGER)) AS DATE) AS shipped_date,
                            CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.expected_delivery_date') AS INTEGER)) AS DATE) AS expected_delivery_date,
                            CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.actual_delivery_date') AS INTEGER)) AS DATE) AS actual_delivery_date,
                            CAST(JSON_VALUE(payload, '$.after.shipping_cost') AS STRING) AS shipping_cost,
                            %s
                        FROM shipments
                        """,
                insertTableDebeziumColumns);
        tableEnv.executeSql(insertSinkSql);
    }
}