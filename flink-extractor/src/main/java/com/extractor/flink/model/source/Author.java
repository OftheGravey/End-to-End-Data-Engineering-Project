package com.extractor.flink.model.source;

public class Author extends DebeziumSourceRecord {
    public Integer authorId;
    public String firstName;
    public String lastName;
    public String biography;
    public String country;

    public Long emittedTsMs;
    public String connectorVersion;
    public String transactionId;
    public Long lsn;

    public Author() {
    }

    @Override
    public String toString() {
        return String.format("Author{authorId=%d, firstName='%s', op='%s'}", authorId, firstName, op);
    }
}