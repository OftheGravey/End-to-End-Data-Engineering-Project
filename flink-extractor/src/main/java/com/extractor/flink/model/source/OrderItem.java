package com.extractor.flink.model.source;

import com.extractor.flink.model.dimensions.BookDimension;
import com.extractor.flink.model.dimensions.CustomerDimension;
import com.extractor.flink.model.dimensions.OrderDimension;

public class OrderItem extends DebeziumSourceRecord {
    public Integer orderItemId;
    public Integer orderId;
    public Integer bookId;
    public Integer quantity;
    public Double priceAtPurchase;
    public Double discount;
    public Long emittedTsMs;
    public String connectorVersion;
    public String transactionId;
    public Long lsn;

    public OrderDimension order;
    public CustomerDimension customer;
    public BookDimension book;

    public OrderItem joinOrder(OrderItem orderItem, OrderDimension orderDimension) {
        orderItem.order = orderDimension;
        return orderItem;
    }
    public OrderItem joinBook(OrderItem orderItem, BookDimension bookDimension) {
        orderItem.book = bookDimension;
        return orderItem;
    }
    public OrderItem joinCustomer(OrderItem orderItem, CustomerDimension customerDimension) {
        orderItem.customer = customerDimension;
        return orderItem;
    }
}