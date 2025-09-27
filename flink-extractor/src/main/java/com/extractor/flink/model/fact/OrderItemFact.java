package com.extractor.flink.model.fact;

import java.util.UUID;

import org.apache.flink.api.common.functions.MapFunction;

import com.extractor.flink.model.source.OrderItem;

import lombok.Data;

@Data
public class OrderItemFact {
    public String orderItemSk;
    public String orderSk;
    public String bookSk;
    public String customerSk;
    public Integer orderItemId;
    public Integer quantity;
    public Double priceAtPurchase;
    public Double discount;
    public Long transactionTime;
    public Double priceTotal;

    public static class OrderItemFactMapping implements MapFunction<OrderItem, OrderItemFact> {
        @Override
        public OrderItemFact map(OrderItem orderItem) {
            OrderItemFact orderItemFact = new OrderItemFact();
            orderItemFact.orderItemSk = UUID.randomUUID().toString();
            orderItemFact.orderSk = orderItem.order.orderSk;
            orderItemFact.bookSk = orderItem.book.bookSk;
            orderItemFact.customerSk = orderItem.customer.customerSk;
            orderItemFact.orderItemId = orderItem.orderItemId;
            orderItemFact.quantity = orderItem.quantity;
            orderItemFact.priceAtPurchase = orderItem.priceAtPurchase;
            orderItemFact.discount = orderItem.discount;
            orderItemFact.transactionTime = orderItem.tsMs;
            orderItemFact.priceTotal = orderItem.quantity * orderItem.priceAtPurchase * (1 - orderItem.discount);
            return orderItemFact;
        }
    }
}