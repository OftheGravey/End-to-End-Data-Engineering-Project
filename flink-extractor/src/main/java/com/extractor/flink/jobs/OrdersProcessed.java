package com.extractor.flink.jobs;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.PostgresDebeziumColumns;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OrdersProcessed {

    static String sourceTopic = "pg-changes.public.orders";
    static String sinkTopic = "parsed.orders";
    static String groupId = "test2";
    public static void main(String[] args) throws Exception {

        // Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceProperties = KafkaProperties.build(sourceTopic, groupId);
        String createTableDebeziumColumns = PostgresDebeziumColumns.createTableFormat();

        String createSourceTableSql = String.format("""
            CREATE TABLE orders (
                payload STRING
            ) %s
            """, sourceProperties);
        System.out.println(createSourceTableSql);
        tableEnv.executeSql(createSourceTableSql);

        String sinkProperties = KafkaProperties.build(sinkTopic, groupId);

        String createSinkTableSql = String.format("""
                CREATE TABLE orders_processed (
                    order_id INTEGER,
                    customer_id INTEGER NOT NULL,
                    order_date TIMESTAMP,
                    status VARCHAR(20) NOT NULL,
                    shipping_method VARCHAR(20),
                    %s
                ) %s
                """, createTableDebeziumColumns, sinkProperties);
        System.out.println(createSinkTableSql);
        tableEnv.executeSql(createSinkTableSql);

        String insertTableDebeziumColumns = PostgresDebeziumColumns.insertTableFormat();
        String insertSinkSql = String.format("""
                INSERT INTO orders_processed
                SELECT 
                    CAST(JSON_VALUE(payload, '$.after.order_id') AS INTEGER) AS order_id,
                    CAST(JSON_VALUE(payload, '$.after.customer_id') AS INTEGER) AS customer_id,
                    CAST(FROM_UNIXTIME(CAST(JSON_VALUE(payload, '$.after.order_date') AS BIGINT)/1000000) AS TIMESTAMP) AS order_date,
                    CAST(JSON_VALUE(payload, '$.after.status') AS VARCHAR(20)) AS status,
                    CAST(JSON_VALUE(payload, '$.after.shipping_method') AS VARCHAR(20)) AS shipping_method,
                    %s
                FROM orders
                """, insertTableDebeziumColumns);
        System.out.println(insertSinkSql);
        tableEnv.executeSql(insertSinkSql);
    }
}
