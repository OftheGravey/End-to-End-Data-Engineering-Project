package com.extractor.flink.jobs.dimensions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.UUID;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.extractor.flink.functions.DebeziumSourceRecord;
import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.PojoSerializer;
import com.extractor.flink.functions.SCD2ProcessFunction;
import com.extractor.flink.functions.TargetDimensionRecord;
import com.extractor.flink.jobs.landing.OrdersLandingJob;
import com.extractor.flink.utils.TopicNameBuilder;

public class OrdersDimensionJob {
    public static String sinkTopic = TopicNameBuilder.build("dimensions.orders");
    static String groupId = System.getenv("GROUP_ID");

    public static class Order extends DebeziumSourceRecord {
        public Integer orderId;
        public Integer customerId;
        public Long orderDate; // timestamp as long
        public String status;
        public String shippingMethod;
        public Long emittedTsMs;
        public String connectorVersion;
        public String transactionId;
        public Long lsn;

        // Default constructor
        public Order() {
        }

        @Override
        public String toString() {
            return String.format("Order{id=%d, customerId=%d, status='%s', op='%s'}",
                    orderId, customerId, status, op);
        }
    }

    public static class OrderDimension extends TargetDimensionRecord {
        public Integer orderId;
        public String status;
        public String shippingMethod;
        public Date orderDate;
        public String orderSk;
        // (customerId) A Kimball rule break - needed because order_items does not have
        //   customerId field. So order item facts would not have direct access to
        //   the customer. Can be filtered out in a presentation layer view
        public Integer customerId; 
        public Timestamp validFrom;
        public Timestamp validTo;

        public OrderDimension(Order record, Long validTo) {
            super(record, validTo);
            this.status = record.status;
            this.customerId = record.customerId;
            this.orderId = record.orderId;
            this.shippingMethod = record.shippingMethod;
            this.orderDate = new Date(record.orderDate);
            this.orderSk = UUID.randomUUID().toString();
            this.validFrom = new Timestamp(record.tsMs);
            this.validTo = new Timestamp(validTo);
        }

        public OrderDimension() {};

        @Override
        public OrderDimension clone(Timestamp validTo) {
            OrderDimension newRecord = new OrderDimension();
            newRecord.status = this.status;
            newRecord.orderId = this.orderId;
            newRecord.customerId = this.customerId;
            newRecord.shippingMethod = this.shippingMethod;
            newRecord.orderDate = this.orderDate;
            newRecord.orderSk = this.orderSk;
            newRecord.validFrom = this.validFrom;
            newRecord.validTo = validTo;
            return newRecord;
        }

        @Override
        public String toString() {
            return String.format("Order(orderId=%d, validFrom=%s, validTo=%s)", orderId,
                    new Timestamp(validFrom.getTime()), new Timestamp(validTo.getTime()));
        }
    }

    public static class OrdersSCD2ProcessFunction extends SCD2ProcessFunction<Order, OrderDimension> {
        public OrdersSCD2ProcessFunction() {
            super(TypeInformation.of(Order.class), TypeInformation.of(OrderDimension.class), OrderDimension::new);
        }
    }

    public static class JsonToOrderMapper implements MapFunction<String, Order> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Order map(String jsonString) throws Exception {
            JsonNode node = objectMapper.readTree(jsonString);
            Order order = new Order();

            order.orderId = node.get("order_id").asInt();
            order.customerId = node.get("customer_id").asInt();
            order.orderDate = node.get("order_date").asLong();
            order.status = node.get("status").asText();
            order.shippingMethod = node.get("shipping_method") != null ? node.get("shipping_method").asText()
                    : null;
            order.op = node.get("op").asText();
            order.emittedTsMs = node.get("emitted_ts_ms").asLong();
            order.tsMs = node.get("ts_ms").asLong();
            order.connectorVersion = node.get("connector_version").asText();
            order.transactionId = node.get("transaction_id").asText();
            order.lsn = node.get("lsn").asLong();

            return order;
        }
    }

    public static void main(String[] args) throws Exception {
        String sourceTopic = OrdersLandingJob.sinkTopic;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaProperties.bootStrapServers)
                .setTopics(sourceTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> rawStream = env.fromSource(source,
                WatermarkStrategy.noWatermarks(),
                "Orders Source");

        DataStream<Order> orderStream = rawStream
                .map(new JsonToOrderMapper())
                .name("Parse JSON to Order");

        DataStream<OrderDimension> scd2Stream = orderStream
                .keyBy(record -> record.orderId)
                .process(new OrdersSCD2ProcessFunction())
                .name("SCD2 Transformation");


        KafkaSink<OrderDimension> sink = KafkaSink.<OrderDimension>builder()
            .setBootstrapServers(KafkaProperties.bootStrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                .setTopic(sinkTopic)
                .setValueSerializationSchema(new PojoSerializer<OrderDimension>())
                .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .build();
 
        // JdbcStatementBuilder<OrderDimension> sinkStatement = (statement, order) -> {
        //     statement.setInt(1, order.orderId);
        //     statement.setString(2, order.status);
        //     statement.setString(3, order.shippingMethod);
        //     statement.setDate(4, order.orderDate);
        //     statement.setString(5, order.orderSk);
        //     statement.setTimestamp(6, order.validFrom);
        //     statement.setTimestamp(7, order.validTo);
        // };

        // JdbcSink<OrderDimension> sink = JdbcSink.<OrderDimension>builder()
        //         .withExecutionOptions(JdbcExecutionOptions.builder()
        //                 .withBatchSize(1000)
        //                 .withBatchIntervalMs(200)
        //                 .withMaxRetries(5)
        //                 .build())
        //         .withQueryStatement(
        //                 """
        //                 insert into modeling_db.d_orders
        //                     (
        //                         orderId, 
        //                         status, 
        //                         shippingMethod, 
        //                         orderDate, 
        //                         orderSk, 
        //                         validFrom, 
        //                         validTo
        //                     ) 
        //                      values (?, ?, ?, ?, ?, ?, ?) 
        //                      ON CONFLICT (ordersk) DO UPDATE SET 
        //                         validTo = EXCLUDED.validTo
        //                 """,
        //                 sinkStatement)
        //         .buildAtLeastOnce(DWConnectionCommonOptions.commonOptions);

        scd2Stream.sinkTo(sink);

        env.execute("d_orders job");

    }
}
