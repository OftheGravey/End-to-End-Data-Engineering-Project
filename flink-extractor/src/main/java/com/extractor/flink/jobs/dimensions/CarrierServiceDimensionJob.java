package com.extractor.flink.jobs.dimensions;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.extractor.flink.functions.DebeziumSourceRecord;
import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.functions.PojoSerializer;
import com.extractor.flink.functions.SCD2MostValidFunction;
import com.extractor.flink.functions.SCD2ProcessFunction;
import com.extractor.flink.functions.TargetDimensionRecord;
import com.extractor.flink.jobs.landing.CarriersLandingJob;
import com.extractor.flink.jobs.landing.ShippingServicesLandingJob;
import com.extractor.flink.utils.DWConnectionCommonOptions;
import com.extractor.flink.utils.TopicNameBuilder;
import com.extractor.flink.functions.CommonFunctions;

public class CarrierServiceDimensionJob {
	static String groupId = System.getenv("GROUP_ID");
	public static String sinkTopic = TopicNameBuilder.build("dimensions.carrier_services");

	public static class Carrier extends DebeziumSourceRecord {
		public Integer carrierId;
		public String name;
		public String contactEmail;
		public String phone;

		// Default constructor
		public Carrier() {
		}

		@Override
		public String toString() {
			return String.format("Carrier{carrierId=%d, op='%s'}", carrierId, op);
		}
	}

	public static class ShippingService extends DebeziumSourceRecord {
		public Integer carrierId;
		public Integer serviceId;
		public String serviceName;
		public Integer estimatedDays;
		public Double costEstimate;

		// Default constructor
		public ShippingService() {
		}

		@Override
		public String toString() {
			return String.format("ShippingService{carrierId=%d, op='%s'}", carrierId, op);
		}
	}

	public static class CarrierService extends DebeziumSourceRecord {
		// Carrier
		public Integer carrierId;
		public String carrierName;
		public String carrierContactEmail;
		public String carrierPhone;
		// Shipping Service
		public Integer serviceId;
		public String serviceName;
		public Integer estimatedDays;
		public Double costEstimate;

		// Default constructor
		public CarrierService() {
		}

		@Override
		public String toString() {
			return String.format("CarrierService{carrierId=%d, serviceId=%d, op='%s'}", carrierId, serviceId, op);
		}
	}

	public static class CarrierServiceDimension extends TargetDimensionRecord {
		public String carrierServiceSk;
		public String serviceName;
		public String carrierName;
		public Integer serviceId;
		public String carrierContactEmail;
		public String carrierPhone;
		public Integer estimatedDays;
		public Double costEstimate;

		public CarrierServiceDimension(CarrierService record, Long validTo) {
			super(record, validTo);
			this.carrierServiceSk = UUID.randomUUID().toString();
			this.serviceName = record.serviceName;
			this.carrierName = record.carrierName;
			this.serviceId = record.serviceId;
			this.carrierContactEmail = record.carrierContactEmail;
			this.carrierPhone = record.carrierPhone;
			this.estimatedDays = record.estimatedDays;
			this.costEstimate = record.costEstimate;
		}

		public CarrierServiceDimension() {
		};

		@Override
		public CarrierServiceDimension clone(Long validTo) {
			CarrierServiceDimension newRecord = new CarrierServiceDimension();

			newRecord.carrierServiceSk = this.carrierServiceSk;
			newRecord.serviceName = this.serviceName;
			newRecord.serviceId = this.serviceId;
			newRecord.carrierName = this.carrierName;
			newRecord.carrierContactEmail = this.carrierContactEmail;
			newRecord.carrierPhone = this.carrierPhone;
			newRecord.estimatedDays = this.estimatedDays;
			newRecord.costEstimate = this.costEstimate;
			newRecord.validFrom = this.validFrom;
			newRecord.validTo = validTo;
			return newRecord;
		}
	}

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

	public static class ShippingServiceJsonParser implements MapFunction<String, ShippingService> {
		private final ObjectMapper objectMapper = new ObjectMapper();

		@Override
		public ShippingService map(String jsonString) throws Exception {
			JsonNode node = objectMapper.readTree(jsonString);
			ShippingService shippingService = new ShippingService();

			shippingService.carrierId = node.get("carrier_id").asInt();
			shippingService.serviceId = node.get("service_id").asInt();
			shippingService.serviceName = node.get("service_name").asText();
			shippingService.estimatedDays = node.get("estimated_days").asInt();
			shippingService.costEstimate = CommonFunctions.base64ToScaledDouble(node.get("cost_estimate").asText(), 2);

			shippingService.op = node.get("op").asText();
			shippingService.tsMs = node.get("ts_ms").asLong();

			return shippingService;
		}
	}

	public static class CarrierJsonParser implements MapFunction<String, Carrier> {
		private final ObjectMapper objectMapper = new ObjectMapper();

		@Override
		public Carrier map(String jsonString) throws Exception {
			JsonNode node = objectMapper.readTree(jsonString);
			Carrier carrier = new Carrier();

			carrier.carrierId = node.get("carrier_id").asInt();
			carrier.name = node.get("name").asText();
			carrier.contactEmail = node.get("contact_email").asText();
			carrier.phone = node.get("phone").asText();

			carrier.op = node.get("op").asText();
			carrier.tsMs = node.get("ts_ms").asLong();

			return carrier;
		}
	}

	// Left join carriers and their shipping services
	// Carriers without services and services without carriers
	// are assumed to never occur - records will be abandoned to the TTL
	public static class CarrierServiceJoinFunction
			extends KeyedCoProcessFunction<Integer, ShippingService, Carrier, CarrierService> {
		private transient MapState<Integer, ShippingService> latestServiceState;
		private transient ValueState<Carrier> latestCarrierState;

		@Override
		public void open(OpenContext ctx) throws Exception {
			latestServiceState = getRuntimeContext().getMapState(new MapStateDescriptor<>("latestService",
					TypeInformation.of(Integer.class), TypeInformation.of(ShippingService.class)));
			latestCarrierState = getRuntimeContext()
					.getState(new ValueStateDescriptor<>("latestCarrier", TypeInformation.of(Carrier.class)));
		}

		@Override
		public void processElement1(ShippingService service, Context context, Collector<CarrierService> out)
				throws Exception {
			latestServiceState.put(service.serviceId, service);

			Carrier currentCarrier = latestCarrierState.value();
			if (currentCarrier != null) {
				out.collect(createJoinedDimension(service, currentCarrier));
			}
		}

		@Override
		public void processElement2(Carrier carrier, Context context, Collector<CarrierService> out) throws Exception {
			latestCarrierState.update(carrier);

			Iterable<Map.Entry<Integer, ShippingService>> services = latestServiceState.entries();
			if (services != null) {
				for (Map.Entry<Integer, ShippingService> entry : services) {
					ShippingService currentService = entry.getValue();
					out.collect(createJoinedDimension(currentService, carrier));
				}
			}
		}

		private CarrierService createJoinedDimension(ShippingService service, Carrier carrier) {
			CarrierService dim = new CarrierService();
			dim.carrierId = carrier.carrierId;
			dim.carrierName = carrier.name;
			dim.carrierContactEmail = carrier.contactEmail;
			dim.carrierPhone = carrier.phone;
			dim.carrierId = service.carrierId;
			dim.serviceId = service.serviceId;
			dim.serviceName = service.serviceName;
			dim.estimatedDays = service.estimatedDays;
			dim.costEstimate = service.costEstimate;
			dim.op = service.op;
			dim.tsMs = service.tsMs;
			return dim;
		}
	}

	public static void main(String[] args) throws Exception {
		String shippingServiceSourceTopic = ShippingServicesLandingJob.sinkTopic;
		String carrierSourceTopic = CarriersLandingJob.sinkTopic;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Books Streams
		KafkaSource<String> shippingServicesSource = KafkaSource.<String>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(shippingServiceSourceTopic)
				.setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> shippingServicesStream = env.fromSource(shippingServicesSource,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((book, timestamp) -> timestamp),
				"Services Source");

		DataStream<ShippingService> shippingService = shippingServicesStream.map(new ShippingServiceJsonParser())
				.name("Parse JSON to Shipping Service")
				.keyBy(book -> book.carrierId);

		// Author Stream
		KafkaSource<String> carrierSource = KafkaSource.<String>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(carrierSourceTopic).setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> carrierRawStream = env.fromSource(carrierSource,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((author, timestamp) -> timestamp),
				"Carrier Source");

		DataStream<Carrier> carrierStream = carrierRawStream.map(new CarrierJsonParser()).name("Parse JSON to Carrier")
				.keyBy(author -> author.carrierId);

		// Join Stream
		DataStream<CarrierService> carrierServiceStream = shippingService.connect(carrierStream)
				.process(new CarrierServiceJoinFunction());

		DataStream<CarrierServiceDimension> scd2Stream = carrierServiceStream.keyBy(record -> record.serviceId)
				.process(new CarrierServicesSCD2ProcessFunction()).name("SCD2 Transformation");

		DataStream<CarrierServiceDimension> scd2StreamWatermarked = scd2Stream.assignTimestampsAndWatermarks(
				WatermarkStrategy.<CarrierServiceDimension>forBoundedOutOfOrderness(Duration.ofSeconds(10))
						.withTimestampAssigner((record, timestamp) -> record.validFrom));

		DataStream<CarrierServiceDimension> bookStreamConsolidated = scd2StreamWatermarked
				.keyBy(record -> record.carrierServiceSk)
				.process(new CarrierServicesSCD2MostValidFunction()).name("Consolidate dimension records");

		KafkaSink<CarrierServiceDimension> streamSink = KafkaSink.<CarrierServiceDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(sinkTopic)
						.setValueSerializationSchema(new PojoSerializer<CarrierServiceDimension>()).build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

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

		bookStreamConsolidated.sinkTo(streamSink);
		bookStreamConsolidated.sinkTo(jdbcSink);

		env.execute("d_carrier_services job");

	}
}
