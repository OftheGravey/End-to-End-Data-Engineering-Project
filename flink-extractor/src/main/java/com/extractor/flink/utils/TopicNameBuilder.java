package com.extractor.flink.utils;

public class TopicNameBuilder {
    public static String build(String root) {
        return root + '.' + System.getenv("ENVIRONMENT");
    } 
}
