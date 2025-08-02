package com.extractor.flink.functions;

public class PostgresDebeziumColumns {
    public static String createTableFormat() {
        return """
                        op CHAR(1),
                        emitted_ts_ms BIGINT,
                        ts_ms BIGINT,
                        connector_version VARCHAR(20),
                        transaction_id VARCHAR(20),
                        lsn BIGINT
                """;
    }

    public static String insertTableFormat() {
        return """
                CAST(JSON_VALUE(payload, '$.op') AS CHAR(1)) AS op,
                CAST(JSON_VALUE(payload, '$.ts_ms') AS BIGINT) AS emitted_ts_ms,
                CAST(JSON_VALUE(payload, '$.source.ts_ms') AS BIGINT) AS ts_ms,
                CAST(JSON_VALUE(payload, '$.source.version') AS VARCHAR(20)) AS connector_version,
                CAST(JSON_VALUE(payload, '$.source.txId') AS VARCHAR(20)) AS transaction_id,
                CAST(JSON_VALUE(payload, '$.source.lsn') AS BIGINT) AS lsn
                    """;
    }
}
