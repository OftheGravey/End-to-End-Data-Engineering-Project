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
import com.extractor.flink.jobs.landing.CarriersLandingJob;
import com.extractor.flink.jobs.landing.ShippingServicesLandingJob;
import com.extractor.flink.join_functions.CarrierShippingServiceJoinFunction;
import com.extractor.flink.model.dimensions.CarrierServiceDimension;
import com.extractor.flink.model.joined.CarrierService;
import com.extractor.flink.model.source.Carrier;
import com.extractor.flink.model.source.ShippingService;
import com.extractor.flink.utils.DWConnectionCommonOptions;
import com.extractor.flink.utils.TopicNameBuilder;

public class CarrierServiceDimensionJob {
	static String groupId = System.getenv("GROUP_ID");
	public static String sinkTopic = TopicNameBuilder.build("dimensions.carrier_services");
	private static final ObjectMapper mapper = new ObjectMapper();

	public static class CarrierServicesSCD2ProcessFunction
			extends SCD2ProcessFunction<CarrierService, CarrierServiceDimension> {
		public CarrierServicesSCD2ProcessFunction() {
			super(TypeInformation.of(CarrierService.class), TypeInformation.of(CarrierServiceDimension.class),
					CarrierServiceDimension::new);
		}
	}

	public static class CarrierServicesSCD2MostValidFunction extends SCD2MostValidFunction<CarrierServiceDimension> {
		public CarrierServicesSCD2MostValidFunction() {
			super(TypeInformation.of(CarrierServiceDimension.class));
		}
	}

	public static void sinkIntoDB(DataStream<CarrierServiceDimension> stream) {

		JdbcStatementBuilder<CarrierServiceDimension> sinkStatement = (statement, carrierService) -> {
			statement.setString(1, carrierService.carrierServiceSk);
			statement.setString(2, carrierService.serviceName);
			statement.setString(3, carrierService.carrierName);
			statement.setInt(4, carrierService.serviceId);
			statement.setString(5, carrierService.carrierContactEmail);
			statement.setString(6, carrierService.carrierPhone);
			statement.setInt(7, carrierService.estimatedDays);
			statement.setDouble(8, carrierService.costEstimate);
			statement.setLong(9, carrierService.validFrom);
			statement.setLong(10, carrierService.validTo);
		};

		JdbcSink<CarrierServiceDimension> jdbcSink = JdbcSink.<CarrierServiceDimension>builder().withExecutionOptions(
				JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(200).withMaxRetries(5).build())
				.withQueryStatement("""
						INSERT INTO modeling_db.d_carrier_services (
						                      carrierServiceSk,
						                      serviceName,
						                      carrierName,
						                      serviceId,
						                      carrierContactEmail,
						                      carrierPhone,
						                      estimatedDays,
						                      costEstimate,
						                      validFrom,
						                      validTo
						) VALUES (?,?,?,?,?,?,?,?,?,?)
						ON CONFLICT (carrierServiceSk) DO UPDATE SET
						validTo = EXCLUDED.validTo
						""", sinkStatement).buildAtLeastOnce(DWConnectionCommonOptions.commonOptions);

		stream.sinkTo(jdbcSink);
	}

	public static void sinkIntoKafka(DataStream<CarrierServiceDimension> stream) {
		KafkaSink<CarrierServiceDimension> streamSink = KafkaSink.<CarrierServiceDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(sinkTopic)
						.setValueSerializationSchema(new PojoSerializer<CarrierServiceDimension>()).build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

		stream.sinkTo(streamSink);
	}

	public static DataStream<ShippingService> shippingServiceStreamInput(StreamExecutionEnvironment env) {
		String shippingServiceSourceTopic = ShippingServicesLandingJob.sinkTopic;
		KafkaSource<String> shippingServicesSource = KafkaSource.<String>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(shippingServiceSourceTopic)
				.setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> shippingServicesStream = env.fromSource(shippingServicesSource,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((book, timestamp) -> timestamp),
				"Services Source");

		DataStream<ShippingService> shippingService = shippingServicesStream
				.map((MapFunction<String, ShippingService>) value -> mapper.readValue(value, ShippingService.class))
				.name("Parse JSON to Shipping Service")
				.keyBy(book -> book.carrierId);

		return shippingService;
	}

	public static DataStream<Carrier> carrierStreamInput(StreamExecutionEnvironment env) {
		String carrierSourceTopic = CarriersLandingJob.sinkTopic;
		KafkaSource<String> carrierSource = KafkaSource.<String>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(carrierSourceTopic).setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> carrierRawStream = env.fromSource(carrierSource,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((author, timestamp) -> timestamp),
				"Carrier Source");

		DataStream<Carrier> carrierStream = carrierRawStream
				.map((MapFunction<String, Carrier>) value -> mapper.readValue(value, Carrier.class))
				.name("Parse JSON to Carrier")
				.keyBy(author -> author.carrierId);

		return carrierStream;
	}

	public static DataStream<CarrierServiceDimension> dimensionTransformation(
			DataStream<ShippingService> shippingServiceStream, DataStream<Carrier> carrierStream) {
		// Join
		DataStream<CarrierService> carrierServiceStream = shippingServiceStream.connect(carrierStream)
				.process(new CarrierShippingServiceJoinFunction());

		// SCD2 State processing
		DataStream<CarrierServiceDimension> scd2Stream = carrierServiceStream
				.keyBy(record -> record.shippingService.serviceId)
				.process(new CarrierServicesSCD2ProcessFunction()).name("SCD2 Transformation");

		DataStream<CarrierServiceDimension> scd2StreamWatermarked = scd2Stream.assignTimestampsAndWatermarks(
				WatermarkStrategy.<CarrierServiceDimension>forBoundedOutOfOrderness(Duration.ofSeconds(10))
						.withTimestampAssigner((record, timestamp) -> record.validFrom));

		// consolidate
		DataStream<CarrierServiceDimension> bookStreamConsolidated = scd2StreamWatermarked
				.keyBy(record -> record.carrierServiceSk)
				.process(new CarrierServicesSCD2MostValidFunction()).name("Consolidate dimension records");

		return bookStreamConsolidated;
	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<ShippingService> shippingServiceStream = shippingServiceStreamInput(env);
		DataStream<Carrier> carrierStream = carrierStreamInput(env);

		DataStream<CarrierServiceDimension> resultStream = dimensionTransformation(shippingServiceStream,
				carrierStream);

		sinkIntoKafka(resultStream);
		sinkIntoDB(resultStream);

		env.execute("d_carrier_services job");

	}
}
