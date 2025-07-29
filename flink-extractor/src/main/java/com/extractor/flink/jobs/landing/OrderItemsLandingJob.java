package com.extractor.flink.jobs.landing;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.PostgresDebeziumColumns;
import com.extractor.flink.utils.TopicNameBuilder;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OrderItemsLandingJob {
    static String sourceTopic = "pg-changes.public.order_items";
    public static String sinkTopic = TopicNameBuilder.build("store.order_items.landing");
    static String groupId = System.getenv("GROUP_ID");
    public static void main(String[] args) throws Exception {

        // Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceProperties = KafkaProperties.build(sourceTopic, groupId);
        String createTableDebeziumColumns = PostgresDebeziumColumns.createTableFormat();

        String createSourceTableSql = String.format("""
            CREATE TABLE order_items (
                payload STRING
            ) %s
            """, sourceProperties);
        System.out.println(createSourceTableSql);
        tableEnv.executeSql(createSourceTableSql);

        String sinkProperties = KafkaProperties.build(sinkTopic, groupId);

        String createSinkTableSql = String.format("""
                CREATE TABLE order_items_processed (
                    order_item_id INTEGER,
                    order_id INTEGER NOT NULL,
                    book_id INTEGER NOT NULL,
                    quantity INTEGER NOT NULL,
                    price_at_purchase STRING,
                    discount STRING,
                    %s
                ) %s
                """, createTableDebeziumColumns, sinkProperties);
        System.out.println(createSinkTableSql);
        tableEnv.executeSql(createSinkTableSql);

        String insertTableDebeziumColumns = PostgresDebeziumColumns.insertTableFormat();
        String insertSinkSql = String.format("""
                INSERT INTO order_items_processed
                SELECT 
                    CAST(JSON_VALUE(payload, '$.after.order_item_id') AS INTEGER) AS order_item_id,
                    CAST(JSON_VALUE(payload, '$.after.order_id') AS INTEGER) AS order_id,
                    CAST(JSON_VALUE(payload, '$.after.book_id') AS INTEGER) AS book_id,
                    CAST(JSON_VALUE(payload, '$.after.quantity') AS INTEGER) AS quantity,
                    CAST(JSON_VALUE(payload, '$.after.price_at_purchase') AS STRING) AS price_at_purchase,
                    CAST(JSON_VALUE(payload, '$.after.discount') AS STRING) AS discount,
                    %s
                FROM order_items
                """, insertTableDebeziumColumns);
        System.out.println(insertSinkSql);
        tableEnv.executeSql(insertSinkSql);
    }
}
