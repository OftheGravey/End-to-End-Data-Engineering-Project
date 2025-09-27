package com.extractor.flink.jobs.dimensions;

import java.time.Duration;

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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.PojoSerializer;
import com.extractor.flink.functions.SCD2MostValidFunction;
import com.extractor.flink.functions.SCD2ProcessFunction;
import com.extractor.flink.jobs.landing.CustomersLandingJob;
import com.extractor.flink.model.dimensions.CustomerDimension;
import com.extractor.flink.model.source.Customer;
import com.extractor.flink.utils.DWConnectionCommonOptions;
import com.extractor.flink.utils.TopicNameBuilder;

public class CustomersDimensionJob {
	static String groupId = System.getenv("GROUP_ID");
	public static String sinkTopic = TopicNameBuilder.build("dimensions.customers");
	private static final ObjectMapper mapper = new ObjectMapper();

	public static class CustomersSCD2ProcessFunction extends SCD2ProcessFunction<Customer, CustomerDimension> {
		public CustomersSCD2ProcessFunction() {
			super(TypeInformation.of(Customer.class), TypeInformation.of(CustomerDimension.class),
					CustomerDimension::new);
		}
	}

	public static class CustomerSCD2MostValidFunction extends SCD2MostValidFunction<CustomerDimension> {
		public CustomerSCD2MostValidFunction() {
			super(TypeInformation.of(CustomerDimension.class));
		}
	}

	public static void sinkIntoDB(DataStream<CustomerDimension> stream) {
		JdbcStatementBuilder<CustomerDimension> sinkStatement = (statement, customer) -> {
			statement.setInt(1, customer.customerId);
			statement.setString(2, customer.email);
			statement.setString(3, customer.phone);
			statement.setString(4, customer.streetAddress);
			statement.setString(5, customer.city);
			statement.setString(6, customer.state);
			statement.setString(7, customer.postalCode);
			statement.setString(8, customer.country);
			statement.setString(9, customer.customerSk);
			statement.setString(10, customer.firstName);
			statement.setString(11, customer.lastName);
			statement.setLong(12, customer.validFrom);
			statement.setLong(13, customer.validTo);
		};

		JdbcSink<CustomerDimension> jdbcSink = JdbcSink.<CustomerDimension>builder().withExecutionOptions(
				JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(200).withMaxRetries(5).build())
				.withQueryStatement("""
						INSERT INTO modeling_db.d_customers (
						customerId,
						email,
						phone,
						streetAddress,
						city,
						state,
						postalCode,
						country,
						customerSk,
						firstName,
						lastName,
						validFrom,
						validTo
						) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
						ON CONFLICT (customerSk) DO UPDATE SET
						validTo = EXCLUDED.validTo
						""", sinkStatement).buildAtLeastOnce(DWConnectionCommonOptions.commonOptions);

		stream.sinkTo(jdbcSink);
	}

	private static void sinkIntoKafka(DataStream<CustomerDimension> stream) {
		KafkaSink<CustomerDimension> streamSink = KafkaSink.<CustomerDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(sinkTopic)
						.setValueSerializationSchema(new PojoSerializer<CustomerDimension>()).build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
		stream.sinkTo(streamSink);
	}

	private static DataStream<Customer> customerStreamInput(StreamExecutionEnvironment env) {
		String sourceTopic = CustomersLandingJob.sinkTopic;
		KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(KafkaProperties.bootStrapServers)
				.setTopics(sourceTopic).setGroupId(groupId).setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema()).build();

		DataStream<String> rawStream = env.fromSource(source,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((customer, timestamp) -> timestamp),
				"Customers Source");

		DataStream<Customer> customerStream = rawStream
				.map((MapFunction<String, Customer>) value -> mapper.readValue(value, Customer.class))
				.name("Parse JSON to Customer");

		return customerStream;
	}

	private static DataStream<CustomerDimension> dimensionTransformation(DataStream<Customer> customerStream) {
		DataStream<CustomerDimension> scd2Stream = customerStream.keyBy(record -> record.customerId)
				.process(new CustomersSCD2ProcessFunction()).name("SCD2 Transformation");

		DataStream<CustomerDimension> scd2StreamWatermarked = scd2Stream.assignTimestampsAndWatermarks(
				WatermarkStrategy.<CustomerDimension>forBoundedOutOfOrderness(Duration.ofSeconds(10))
						.withTimestampAssigner((record, timestamp) -> record.validFrom));

		DataStream<CustomerDimension> customerConsolidated = scd2StreamWatermarked.keyBy(record -> record.customerSk)
				.process(new CustomerSCD2MostValidFunction()).name("Customer dimension consolidation");

		return customerConsolidated;
	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Customer> customerStream = customerStreamInput(env);

		DataStream<CustomerDimension> resultStream = dimensionTransformation(customerStream);

		sinkIntoKafka(resultStream);
		sinkIntoDB(resultStream);

		env.execute("d_customers job");

	}
}
