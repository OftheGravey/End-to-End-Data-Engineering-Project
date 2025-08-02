package com.extractor.flink.utils;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

public class DWConnectionCommonOptions {
    public static JdbcConnectionOptions commonOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
            .withUrl("jdbc:postgresql://postgres-dw:5433/dw_db")
            .withDriverName("org.postgresql.Driver")
            .withUsername("postgres")
            .withPassword("postgres")
            .build();
}
