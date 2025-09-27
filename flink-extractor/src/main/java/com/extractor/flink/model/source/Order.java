package com.extractor.flink.model.source;

public class Order extends DebeziumSourceRecord {
    public Integer orderId;
    public Integer customerId;
    public Long orderDate; // timestamp as long
    public String status;
    public String shippingMethod;
    public Long emittedTsMs;
    public String connectorVersion;
    public String transactionId;
    public Long lsn;
}