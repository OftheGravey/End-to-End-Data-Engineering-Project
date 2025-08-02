package com.extractor.flink.jobs.landing;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.PostgresDebeziumColumns;
import com.extractor.flink.utils.TopicNameBuilder;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class AuthorsLandingJob {
    static String sourceTopic = "pg-changes.public.authors";
    public static String sinkTopic = TopicNameBuilder.build("store.authors.landing");
    static String groupId = System.getenv("GROUP_ID");
    public static void main(String[] args) throws Exception {

        // Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceProperties = KafkaProperties.build(sourceTopic, groupId);
        String createTableDebeziumColumns = PostgresDebeziumColumns.createTableFormat();

        String createSourceTableSql = String.format("""
            CREATE TABLE authors (
                payload STRING
            ) %s
            """, sourceProperties);
        System.out.println(createSourceTableSql);
        tableEnv.executeSql(createSourceTableSql);

        String sinkProperties = KafkaProperties.build(sinkTopic, groupId);

        String createSinkTableSql = String.format("""
                CREATE TABLE authors_processed (
                    author_id INTEGER,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255),
                    biography VARCHAR(2048),
                    country VARCHAR(255),
                    %s
                ) %s
                """, createTableDebeziumColumns, sinkProperties);
        System.out.println(createSinkTableSql);
        tableEnv.executeSql(createSinkTableSql);

        String insertTableDebeziumColumns = PostgresDebeziumColumns.insertTableFormat();
        String insertSinkSql = String.format("""
                INSERT INTO authors_processed
                SELECT 
                    CAST(JSON_VALUE(payload, '$.after.author_id') AS INTEGER) AS author_id,
                    CAST(JSON_VALUE(payload, '$.after.first_name') AS VARCHAR(50)) AS first_name,
                    CAST(JSON_VALUE(payload, '$.after.last_name') AS VARCHAR(50)) AS last_name,
                    CAST(JSON_VALUE(payload, '$.after.biography') AS VARCHAR(50)) AS biography,
                    CAST(JSON_VALUE(payload, '$.after.country') AS VARCHAR(50)) AS country,
                    %s
                FROM authors
                """, insertTableDebeziumColumns);
        System.out.println(insertSinkSql);
        tableEnv.executeSql(insertSinkSql);
    }
}
