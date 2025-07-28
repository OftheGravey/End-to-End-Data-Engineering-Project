package com.extractor.flink.jobs.facts;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import com.extractor.flink.functions.DebeziumSourceRecord;
import com.extractor.flink.functions.FactDimensionJoiner;
import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.jobs.dimensions.BooksDimensionJob;
import com.extractor.flink.jobs.dimensions.CustomersDimensionJob;
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob;
import com.extractor.flink.jobs.dimensions.BooksDimensionJob.Author;
import com.extractor.flink.jobs.dimensions.BooksDimensionJob.Book;
import com.extractor.flink.jobs.dimensions.BooksDimensionJob.BookAuthor;
import com.extractor.flink.jobs.dimensions.BooksDimensionJob.BookDimension;
import com.extractor.flink.jobs.dimensions.CustomersDimensionJob.CustomerDimension;
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob.Order;
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob.OrderDimension;
import com.extractor.flink.jobs.facts.OrderItemsFactJob.OrderItem;
import com.extractor.flink.jobs.landing.OrdersLandingJob;
import com.extractor.flink.utils.TopicNameBuilder;
import com.extractor.flink.functions.PojoDeserializer;
import com.extractor.flink.functions.PojoSerializer;

public class OrderItemsFactJob {
    public static class OrderItemWithOrder extends OrderItem {
        public String orderSk;
        public Integer customerId;

        public static OrderItemWithOrder createJoinedRecord(OrderItem orderItem, OrderDimension order) {
            OrderItemWithOrder record = new OrderItemWithOrder();
            record.tsMs = orderItem.tsMs;
            record.orderItemId = orderItem.orderItemId;
            record.orderId = orderItem.orderId;
            record.bookId = orderItem.bookId;
            record.quantity = orderItem.quantity;
            record.priceAtPurchase = orderItem.priceAtPurchase;
            record.discount = orderItem.discount;

            // New assignment
            record.orderSk = order.orderSk;
            record.customerId = order.customerId;

            return record;
        }
    }

    public static class OrderItemWithCustomer extends OrderItemWithOrder {
        public String customerSk;

        public static OrderItemWithCustomer createJoinedRecord(OrderItemWithOrder orderItem,
                CustomerDimension customer) {
            OrderItemWithCustomer record = new OrderItemWithCustomer();
            record.tsMs = orderItem.tsMs;
            record.orderItemId = orderItem.orderItemId;
            record.orderId = orderItem.orderId;
            record.bookId = orderItem.bookId;
            record.quantity = orderItem.quantity;
            record.priceAtPurchase = orderItem.priceAtPurchase;
            record.discount = orderItem.discount;
            record.orderSk = orderItem.orderSk;

            // New
            record.customerSk = customer.customerSk;

            return record;
        }
    }

    public static class OrderItemWithBook extends OrderItemWithCustomer {
        public String bookSk;

        public static OrderItemWithBook createJoinedRecord(OrderItemWithCustomer orderItem, BookDimension book) {
            OrderItemWithBook record = new OrderItemWithBook();
            record.tsMs = orderItem.tsMs;
            record.orderItemId = orderItem.orderItemId;
            record.orderId = orderItem.orderId;
            record.bookId = orderItem.bookId;
            record.quantity = orderItem.quantity;
            record.priceAtPurchase = orderItem.priceAtPurchase;
            record.discount = orderItem.discount;
            record.orderSk = orderItem.orderSk;
            record.customerSk = orderItem.customerSk;

            // New
            record.bookSk = book.bookSk;

            return record;
        }
    }

    public static class OrderItemFactMapping implements MapFunction<OrderItemWithBook, OrderItemFact> {
        @Override
        public OrderItemFact map(OrderItemWithBook orderItem) {
            OrderItemFact orderItemFact = new OrderItemFact();
            orderItemFact.orderItemSk = UUID.randomUUID().toString();
            orderItemFact.orderSk = orderItem.orderSk;
            orderItemFact.bookSk = orderItem.bookSk;
            orderItemFact.customerSk = orderItem.customerSk;
            orderItemFact.orderItemId = orderItem.orderItemId;
            orderItemFact.quantity = orderItem.quantity;
            orderItemFact.priceAtPurchase = orderItem.priceAtPurchase;
            orderItemFact.discount = orderItem.discount;
            orderItemFact.transactionTime = new Timestamp(orderItem.tsMs);
            orderItemFact.priceTotal = orderItem.quantity * orderItem.priceAtPurchase * (1 - orderItem.discount);
            return orderItemFact;
        }
    }

    public static class OrderItemFact {
        public String orderItemSk;
        public String orderSk;
        public String bookSk;
        public String customerSk;
        public Integer orderItemId;
        public Integer quantity;
        public Double priceAtPurchase;
        public Double discount;
        public Timestamp transactionTime;
        public Double priceTotal;
    }

    public static class OrderItem extends DebeziumSourceRecord {
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
    }

    public static class OrderItemJsonParser implements MapFunction<String, OrderItem> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        private double base64ToScaledDouble(String base64String, int scale) {
            byte[] decoded = Base64.getDecoder().decode(base64String);
            if (decoded.length < 8) {
                return 0.0;
            }
            double value = ByteBuffer.wrap(decoded).getDouble();
            BigDecimal bd = BigDecimal.valueOf(value)
                    .setScale(scale, RoundingMode.HALF_UP);
            return bd.doubleValue();
        }

        @Override
        public OrderItem map(String jsonString) throws Exception {
            JsonNode node = objectMapper.readTree(jsonString);
            OrderItem orderItem = new OrderItem();

            orderItem.orderItemId = node.get("orderItemId").asInt();
            orderItem.orderId = node.get("orderId").asInt();
            orderItem.bookId = node.get("bookId").asInt();
            orderItem.quantity = node.get("quantity").asInt();
            orderItem.priceAtPurchase = base64ToScaledDouble(node.get("priceAtPurchase").asText(), 2);
            orderItem.discount = base64ToScaledDouble(node.get("discount").asText(), 2);

            orderItem.op = node.get("op").asText();
            orderItem.emittedTsMs = node.get("emitted_ts_ms").asLong();
            orderItem.tsMs = node.get("ts_ms").asLong();
            orderItem.connectorVersion = node.get("connector_version").asText();
            orderItem.transactionId = node.get("transaction_id").asText();
            orderItem.lsn = node.get("lsn").asLong();

            return orderItem;
        }
    }

    public static class OrderItemOrderJoinFunction
            extends FactDimensionJoiner<OrderItem, OrderDimension, OrderItemWithOrder> {
        public OrderItemOrderJoinFunction() {
            super(TypeInformation.of(OrderItem.class), TypeInformation.of(OrderDimension.class));
        }

        public OrderItemWithOrder createJoinedDimension(OrderItem orderItem, OrderDimension order) {
            OrderItemWithOrder joinedRecord = OrderItemWithOrder.createJoinedRecord(orderItem, order);
            return joinedRecord;
        }
    }

    public static class OrderItemCustomerJoinFunction
            extends FactDimensionJoiner<OrderItemWithOrder, CustomerDimension, OrderItemWithCustomer> {
        public OrderItemCustomerJoinFunction() {
            super(TypeInformation.of(OrderItemWithOrder.class), TypeInformation.of(CustomerDimension.class));
        }

        public OrderItemWithCustomer createJoinedDimension(OrderItemWithOrder orderItem, CustomerDimension customer) {
            OrderItemWithCustomer joinedRecord = OrderItemWithCustomer.createJoinedRecord(orderItem, customer);
            return joinedRecord;
        }
    }

    public static class OrderItemBookJoinFunction
            extends FactDimensionJoiner<OrderItemWithCustomer, BookDimension, OrderItemWithBook> {
        public OrderItemBookJoinFunction() {
            super(TypeInformation.of(OrderItemWithCustomer.class), TypeInformation.of(BookDimension.class));
        }

        public OrderItemWithBook createJoinedDimension(OrderItemWithCustomer orderItem, BookDimension book) {
            OrderItemWithBook joinedRecord = OrderItemWithBook.createJoinedRecord(orderItem, book);
            return joinedRecord;
        }
    }

    public static void main(String[] args) throws Exception {
        String groupId = System.getenv("GROUP_ID");
        String sourceTopic = OrdersLandingJob.sinkTopic;
        String sinkTopic = TopicNameBuilder.build("facts.order_items");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Order Dimension
        String orderDimensionTopic = OrdersDimensionJob.sinkTopic;
        KafkaSource<OrderDimension> orderDimensionSource = KafkaSource.<OrderDimension>builder()
                .setBootstrapServers(KafkaProperties.bootStrapServers)
                .setTopics(orderDimensionTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new PojoDeserializer<OrderDimension>(OrderDimension.class))
                .build();
        DataStream<OrderDimension> orderDimensionStream = env.fromSource(orderDimensionSource,
                WatermarkStrategy.noWatermarks(), "Orders Source")
                .keyBy(order -> order.orderId);

        // Customer Dimension
        String customerDimensionTopic = CustomersDimensionJob.sinkTopic;
        KafkaSource<CustomerDimension> customerDimensionSource = KafkaSource.<CustomerDimension>builder()
                .setBootstrapServers(KafkaProperties.bootStrapServers)
                .setTopics(customerDimensionTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new PojoDeserializer<CustomerDimension>(CustomerDimension.class))
                .build();
        DataStream<CustomerDimension> customerDimensionStream = env.fromSource(customerDimensionSource,
                WatermarkStrategy.noWatermarks(), "Customer Source")
                .keyBy(customer -> customer.customerId);

        // Book Dimension
        String bookDimensionTopic = BooksDimensionJob.sinkTopic;
        KafkaSource<BookDimension> bookDimensionSource = KafkaSource.<BookDimension>builder()
                .setBootstrapServers(KafkaProperties.bootStrapServers)
                .setTopics(bookDimensionTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new PojoDeserializer<BookDimension>(BookDimension.class))
                .build();
        DataStream<BookDimension> bookDimensionStream = env.fromSource(bookDimensionSource,
                WatermarkStrategy.noWatermarks(), "Book Source")
                .keyBy(book -> book.bookId);

        // Order item from Kafka
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaProperties.bootStrapServers)
                .setTopics(sourceTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> orderItemRawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(),
                "Order item source");

        DataStream<OrderItem> orderItemStream = orderItemRawStream.map(new OrderItemJsonParser())
                .name("Parse JSON to Order Item")
                .keyBy(orderItem -> orderItem.orderId);

        // Enrich order item facts
        DataStream<OrderItemWithBook> orderItemEnriched = orderItemStream
                .connect(orderDimensionStream)
                .process(new OrderItemOrderJoinFunction())
                .connect(customerDimensionStream).process(new OrderItemCustomerJoinFunction())
                .connect(bookDimensionStream)
                .process(new OrderItemBookJoinFunction());

        DataStream<OrderItemFact> orderFacts = orderItemEnriched.map(new OrderItemFactMapping());

        KafkaSink<OrderItemFact> sink = KafkaSink.<OrderItemFact>builder()
                .setBootstrapServers(KafkaProperties.bootStrapServers)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(sinkTopic)
                                .setValueSerializationSchema(new PojoSerializer<OrderItemFact>())
                                .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build();

        orderFacts.sinkTo(sink);

    }
}
