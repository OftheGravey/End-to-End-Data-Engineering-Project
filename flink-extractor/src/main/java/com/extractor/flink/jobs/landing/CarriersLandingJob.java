package com.extractor.flink.jobs.landing;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.MySQLDebeziumColumns;
import com.extractor.flink.utils.TopicNameBuilder;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CarriersLandingJob {

    static String sourceTopic = "mysql-changes.shipping_db.carriers";
    public static String sinkTopic = TopicNameBuilder.build("shipping.carriers.landing");
    static String groupId = System.getenv("GROUP_ID");

    public static void main(String[] args) throws Exception {

        // Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceProperties = KafkaProperties.build(sourceTopic, groupId);
        String createTableDebeziumColumns = MySQLDebeziumColumns.createTableFormat();

        String createSourceTableSql = String.format("""
                CREATE TABLE carriers (
                    payload STRING
                ) %s
                """, sourceProperties);
        tableEnv.executeSql(createSourceTableSql);

        String sinkProperties = KafkaProperties.build(sinkTopic, groupId);

        String createSinkTableSql = String.format("""
                CREATE TABLE carriers_processed (
                    carrier_id INTEGER,
                    name STRING,
                    contact_email STRING,
                    phone VARCHAR(12),
                    %s
                ) %s
                """, createTableDebeziumColumns, sinkProperties);
        tableEnv.executeSql(createSinkTableSql);

        String insertTableDebeziumColumns = MySQLDebeziumColumns.insertTableFormat();
        String insertSinkSql = String.format("""
                INSERT INTO carriers_processed
                SELECT
                    CAST(JSON_VALUE(payload, '$.after.carrier_id') AS INTEGER) AS carrier_id,
                    CAST(JSON_VALUE(payload, '$.after.name') AS STRING) AS name,
                    CAST(JSON_VALUE(payload, '$.after.contact_email') AS STRING) AS contact_email,
                    CAST(JSON_VALUE(payload, '$.after.phone') AS VARCHAR(12)) AS phone,
                    %s
                FROM carriers
                """, insertTableDebeziumColumns);
        tableEnv.executeSql(insertSinkSql);
    }
}