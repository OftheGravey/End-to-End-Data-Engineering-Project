package com.extractor.flink.jobs.dimensions;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.UUID;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
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
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob.OrderDimension;
import com.extractor.flink.jobs.landing.CustomersLandingJob;
import com.extractor.flink.utils.DWConnectionCommonOptions;
import com.extractor.flink.utils.TopicNameBuilder;

public class CustomersDimensionJob {
    static String groupId = System.getenv("GROUP_ID");
    public static String sinkTopic = TopicNameBuilder.build("dimensions.customers");

    public static class Customer extends DebeziumSourceRecord {
        public Integer customerId;
        public String firstName;
        public String lastName;
        public String email;
        public String phone;
        public Timestamp createdAt;
        public String streetAddress;
        public String city;
        public String state;
        public String postalCode;
        public String country;
        public Long emittedTsMs;
        public String connectorVersion;
        public String transactionId;
        public Long lsn;

        // Default constructor
        public Customer() {
        }

        @Override
        public String toString() {
            return String.format("Customer{customerId=%d, city='%s', op='%s'}",
                    customerId, city, op);
        }
    }

    public static class CustomerDimension extends TargetDimensionRecord {
        public Integer customerId;
        public String email;
        public String phone;
        public String streetAddress;
        public String city;
        public String state;
        public String postalCode;
        public String country;
        public String customerSk;
        public String firstName;
        public String lastName;
        public Timestamp validFrom;
        public Timestamp validTo;

        public CustomerDimension(Customer record, Long validTo) {
            super(record, validTo);
            this.customerId = record.customerId;
            this.email = record.email;
            this.phone = record.phone;
            this.streetAddress = record.streetAddress;
            this.city = record.city;
            this.state = record.state;
            this.postalCode = record.postalCode;
            this.country = record.country;
            this.firstName = record.firstName;
            this.lastName = record.lastName;
            this.customerSk = UUID.randomUUID().toString();
            this.validFrom = new Timestamp(record.tsMs);
            this.validTo = new Timestamp(validTo);
        }

        public CustomerDimension(){};

        @Override
        public CustomerDimension clone(Timestamp validTo) {
            CustomerDimension newRecord = new CustomerDimension();
            newRecord.customerId = this.customerId;
            newRecord.email = this.email;
            newRecord.phone = this.phone;
            newRecord.streetAddress = this.streetAddress;
            newRecord.city = this.city;
            newRecord.state = this.state;
            newRecord.postalCode = this.postalCode;
            newRecord.country = this.country;
            newRecord.firstName = this.firstName;
            newRecord.lastName = this.lastName;
            newRecord.customerSk = this.customerSk;
            newRecord.validFrom = this.validFrom;
            newRecord.validTo = validTo;
            return newRecord;
        }
    }

    public static class OrdersSCD2ProcessFunction extends SCD2ProcessFunction<Customer, CustomerDimension> {
        public OrdersSCD2ProcessFunction() {
            super(TypeInformation.of(Customer.class), TypeInformation.of(CustomerDimension.class),
                    CustomerDimension::new);
        }
    }

    public static class JsonToOrderMapper implements MapFunction<String, Customer> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Customer map(String jsonString) throws Exception {
            JsonNode node = objectMapper.readTree(jsonString);
            Customer customer = new Customer();

            customer.customerId = node.get("customer_id").asInt();
            customer.firstName = node.get("first_name").asText();
            customer.lastName = node.get("last_name").asText();
            customer.email = node.get("email").asText();
            customer.phone = node.get("phone").asText();
            customer.createdAt = new Timestamp(node.get("created_at").asLong());
            customer.streetAddress = node.get("street_address").asText();
            customer.city = node.get("city").asText();
            customer.state = node.get("state").asText();
            customer.postalCode = node.get("postal_code").asText();
            customer.country = node.get("country").asText();

            customer.op = node.get("op").asText();
            customer.emittedTsMs = node.get("emitted_ts_ms").asLong();
            customer.tsMs = node.get("ts_ms").asLong();
            customer.connectorVersion = node.get("connector_version").asText();
            customer.transactionId = node.get("transaction_id").asText();
            customer.lsn = node.get("lsn").asLong();

            return customer;
        }
    }

    public static void main(String[] args) throws Exception {
        String sourceTopic = CustomersLandingJob.sinkTopic;

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

        DataStream<Customer> orderStream = rawStream
                .map(new JsonToOrderMapper())
                .name("Parse JSON to Order");

        DataStream<CustomerDimension> scd2Stream = orderStream
                .keyBy(record -> record.customerId)
                .process(new OrdersSCD2ProcessFunction())
                .name("SCD2 Transformation");

        KafkaSink<CustomerDimension> sink = KafkaSink.<CustomerDimension>builder()
            .setBootstrapServers(KafkaProperties.bootStrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                .setTopic(sinkTopic)
                .setValueSerializationSchema(new PojoSerializer<CustomerDimension>())
                .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .build();

        // JdbcStatementBuilder<CustomerDimension> sinkStatement = (statement, customer) -> {
        //     statement.setInt(1, customer.customerId);
        //     statement.setString(2, customer.email);
        //     statement.setString(3, customer.phone);
        //     statement.setString(4, customer.streetAddress);
        //     statement.setString(5, customer.city);
        //     statement.setString(6, customer.state);
        //     statement.setString(7, customer.postalCode);
        //     statement.setString(8, customer.country);
        //     statement.setString(9, customer.customerSk);
        //     statement.setString(10, customer.firstName);
        //     statement.setString(11, customer.lastName);
        //     statement.setTimestamp(12, customer.validFrom);
        //     statement.setTimestamp(13, customer.validTo);
        // };

        // JdbcSink<CustomerDimension> sink = JdbcSink.<CustomerDimension>builder()
        //         .withExecutionOptions(JdbcExecutionOptions.builder()
        //                 .withBatchSize(1000)
        //                 .withBatchIntervalMs(200)
        //                 .withMaxRetries(5)
        //                 .build())
        //         .withQueryStatement(
        //                 """
        //                 INSERT INTO modeling_db.d_customers (
        //                     customerId,
        //                     email,
        //                     phone,
        //                     streetAddress, 
        //                     city, 
        //                     state, 
        //                     postalCode, 
        //                     country,
        //                     customerSk, 
        //                     firstName, 
        //                     lastName, 
        //                     validFrom, 
        //                     validTo
        //                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        //                  ON CONFLICT (customerSk) DO UPDATE SET
        //                     validTo = EXCLUDED.validTo
        //                 """,
        //                 sinkStatement)
        //         .buildAtLeastOnce(DWConnectionCommonOptions.commonOptions);

        scd2Stream.sinkTo(sink);

        env.execute("d_customers job");

    }
}
