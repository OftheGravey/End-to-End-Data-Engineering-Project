package com.extractor.flink.jobs.landing;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.MySQLDebeziumColumns;
import com.extractor.flink.utils.TopicNameBuilder;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ShipmentEventsLandingJob {

    static String sourceTopic = "mysql-changes.shipping_db.shipment_events";
    public static String sinkTopic = TopicNameBuilder.build("shipping.shipment_events.landing");
    static String groupId = System.getenv("GROUP_ID");

    public static void main(String[] args) throws Exception {

        // Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceProperties = KafkaProperties.build(sourceTopic, groupId);
        String createTableDebeziumColumns = MySQLDebeziumColumns.createTableFormat();

        String createSourceTableSql = String.format("""
                CREATE TABLE shipment_events (
                    payload STRING
                ) %s
                """, sourceProperties);
        tableEnv.executeSql(createSourceTableSql);

        String sinkProperties = KafkaProperties.build(sinkTopic, groupId);

        String createSinkTableSql = String.format("""
                CREATE TABLE shipment_events_processed (
                    event_id INTEGER,
                    shipment_id INTEGER,
                    status STRING,
                    location STRING,
                    %s
                ) %s
                """, createTableDebeziumColumns, sinkProperties);
        tableEnv.executeSql(createSinkTableSql);

        String insertTableDebeziumColumns = MySQLDebeziumColumns.insertTableFormat();
        String insertSinkSql = String.format("""
                INSERT INTO shipment_events_processed
                SELECT
                    CAST(JSON_VALUE(payload, '$.after.event_id') AS INTEGER) AS event_id,
                    CAST(JSON_VALUE(payload, '$.after.shipment_id') AS INTEGER) AS shipment_id,
                    CAST(JSON_VALUE(payload, '$.after.status') AS STRING) AS status,
                    CAST(JSON_VALUE(payload, '$.after.location') AS STRING) AS location,
                    %s
                FROM shipment_events
                """, insertTableDebeziumColumns);
        tableEnv.executeSql(insertSinkSql);
    }
}