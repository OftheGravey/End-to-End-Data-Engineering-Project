package com.extractor.flink.functions;

public class KafkaProperties {
    public static String bootStrapServers = "kafka:9092";
    public static String build(String topic, String groupId) {
        String result = String.format("""
                 WITH (
                    'connector' = 'kafka',
                    'topic' = '%s',
                    'properties.bootstrap.servers' = '%s',
                    'properties.group.id' = '%s',
                    'format' = 'json',
                    'scan.startup.mode' = 'earliest-offset'
                )
                """, topic, bootStrapServers, groupId);

        return result;
    }
}
