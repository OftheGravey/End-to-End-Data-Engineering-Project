package com.extractor.flink.jobs.landing;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.PostgresDebeziumColumns;
import com.extractor.flink.utils.TopicNameBuilder;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class BooksLandingJob {
    static String sourceTopic = "pg-changes.public.books";
    public static String sinkTopic = TopicNameBuilder.build("store.books.landing");
    static String groupId = System.getenv("GROUP_ID");
    public static void main(String[] args) throws Exception {

        // Set up execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sourceProperties = KafkaProperties.build(sourceTopic, groupId);
        String createTableDebeziumColumns = PostgresDebeziumColumns.createTableFormat();

        String createSourceTableSql = String.format("""
            CREATE TABLE books (
                payload STRING
            ) %s
            """, sourceProperties);
        System.out.println(createSourceTableSql);
        tableEnv.executeSql(createSourceTableSql);

        String sinkProperties = KafkaProperties.build(sinkTopic, groupId);

        String createSinkTableSql = String.format("""
                CREATE TABLE books_processed (
                    book_id INTEGER,
                    title VARCHAR(50),
                    author_id INTEGER,
                    isbn VARCHAR(50),
                    price STRING,
                    published_date DATE,
                    description VARCHAR(500),
                    genre VARCHAR(50),
                    stock INTEGER,
                    %s
                ) %s
                """, createTableDebeziumColumns, sinkProperties);
        System.out.println(createSinkTableSql);
        tableEnv.executeSql(createSinkTableSql);
        String insertTableDebeziumColumns = PostgresDebeziumColumns.insertTableFormat();
        String insertSinkSql = String.format("""
                INSERT INTO books_processed
                SELECT 
                    CAST(JSON_VALUE(payload, '$.after.book_id') AS INTEGER) AS book_id,
                    CAST(JSON_VALUE(payload, '$.after.title') AS VARCHAR(50)) AS title,
                    CAST(JSON_VALUE(payload, '$.after.author_id') AS INTEGER) AS author_id,
                    CAST(JSON_VALUE(payload, '$.after.isbn') AS VARCHAR(13)) AS isbn,
                    CAST(JSON_VALUE(payload, '$.after.price') AS STRING) AS price,
                    CAST(FROM_UNIXTIME((60 * 60 * 24) * CAST(JSON_VALUE(payload, '$.after.published_date') AS INTEGER)) AS DATE) AS published_date,
                    CAST(JSON_VALUE(payload, '$.after.description') AS VARCHAR(500)) AS description,
                    CAST(JSON_VALUE(payload, '$.after.genre') AS VARCHAR(50)) AS genre,
                    CAST(JSON_VALUE(payload, '$.after.stock') AS INTEGER) AS stock,
                    %s
                FROM books
                """, insertTableDebeziumColumns);
        System.out.println(insertSinkSql);
        tableEnv.executeSql(insertSinkSql);
    }
}
