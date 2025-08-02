package com.extractor.flink.jobs.landing;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.PostgresDebeziumColumns;
import com.extractor.flink.utils.TopicNameBuilder;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CustomersLandingJob {
    static String sourceTopic = "pg-changes.public.customers";
    public static String sinkTopic = TopicNameBuilder.build("store.customers.landing");
    static String groupId = System.getenv("GROUP_ID");
    public static void main(String[] args) throws Exception {

        // Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceProperties = KafkaProperties.build(sourceTopic, groupId);
        String createTableDebeziumColumns = PostgresDebeziumColumns.createTableFormat();

        String createSourceTableSql = String.format("""
            CREATE TABLE customers (
                payload STRING
            ) %s
            """, sourceProperties);
        System.out.println(createSourceTableSql);
        tableEnv.executeSql(createSourceTableSql);

        String sinkProperties = KafkaProperties.build(sinkTopic, groupId);

        String createSinkTableSql = String.format("""
                CREATE TABLE customers_processed (
                    customer_id INTEGER,
                    first_name VARCHAR(50),
                    last_name VARCHAR(50),
                    email VARCHAR(50),
                    phone VARCHAR(16),
                    created_at TIMESTAMP,
                    street_address VARCHAR(50),
                    city VARCHAR(50),
                    state VARCHAR(50),
                    postal_code VARCHAR(10),
                    country VARCHAR(50),
                    %s
                ) %s
                """, createTableDebeziumColumns, sinkProperties);
        System.out.println(createSinkTableSql);
        tableEnv.executeSql(createSinkTableSql);

        String insertTableDebeziumColumns = PostgresDebeziumColumns.insertTableFormat();
        String insertSinkSql = String.format("""
                INSERT INTO customers_processed
                SELECT 
                    CAST(JSON_VALUE(payload, '$.after.customer_id') AS INTEGER) AS customer_id,
                    CAST(JSON_VALUE(payload, '$.after.first_name') AS VARCHAR(50)) AS first_name,
                    CAST(JSON_VALUE(payload, '$.after.last_name') AS VARCHAR(50)) AS last_name,
                    CAST(JSON_VALUE(payload, '$.after.email') AS VARCHAR(50)) AS email,
                    CAST(JSON_VALUE(payload, '$.after.phone') AS VARCHAR(20)) AS phone,
                    CAST(FROM_UNIXTIME(CAST(JSON_VALUE(payload, '$.after.created_at') AS BIGINT)/1000000) AS TIMESTAMP) AS created_at,
                    CAST(JSON_VALUE(payload, '$.after.street_address') AS VARCHAR(50)) AS street_address,
                    CAST(JSON_VALUE(payload, '$.after.city') AS VARCHAR(50)) AS city,
                    CAST(JSON_VALUE(payload, '$.after.state') AS VARCHAR(50)) AS state,
                    CAST(JSON_VALUE(payload, '$.after.postal_code') AS VARCHAR(10)) AS postal_code,
                    CAST(JSON_VALUE(payload, '$.after.country') AS VARCHAR(50)) AS country,
                    %s
                FROM customers
                """, insertTableDebeziumColumns);
        System.out.println(insertSinkSql);
        tableEnv.executeSql(insertSinkSql);
    }
}
