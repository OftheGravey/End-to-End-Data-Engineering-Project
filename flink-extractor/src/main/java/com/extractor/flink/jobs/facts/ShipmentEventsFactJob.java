package com.extractor.flink.jobs.facts;

import java.sql.Date;
import java.time.Duration;
import java.time.LocalDate;
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
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.extractor.flink.functions.CommonFunctions;
import com.extractor.flink.functions.DebeziumSourceRecord;
import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.jobs.dimensions.CarrierServiceDimensionJob;
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob;
import com.extractor.flink.jobs.dimensions.CarrierServiceDimensionJob.CarrierServiceDimension;
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob.OrderDimension;
import com.extractor.flink.jobs.landing.ShipmentEventsLandingJob;
import com.extractor.flink.jobs.landing.ShipmentsLandingJob;
import com.extractor.flink.utils.DWConnectionCommonOptions;
import com.extractor.flink.utils.TopicNameBuilder;

import lombok.Data;

import com.extractor.flink.functions.PojoDeserializer;
import com.extractor.flink.functions.PojoSerializer;

public class ShipmentEventsFactJob {
	private static final Logger LOG = LoggerFactory.getLogger(OrderItemsFactJob.class);

	public static class ShipmentEventFactMapping implements MapFunction<ShipmentEvent, ShipmentEventFact> {
		@Override
		public ShipmentEventFact map(ShipmentEvent shipmentEvent) {
			ShipmentEventFact shipmentEventFact = new ShipmentEventFact();
			shipmentEventFact.shipmentEventSk = UUID.randomUUID().toString();
			shipmentEventFact.orderSk = shipmentEvent.order.orderSk;
			shipmentEventFact.carrierServiceSk = shipmentEvent.carrierService.carrierServiceSk;
			shipmentEventFact.shipmentId = shipmentEvent.shipmentId;
			shipmentEventFact.shipmentEventId = shipmentEvent.shipmentEventId;
			shipmentEventFact.status = shipmentEvent.status;
			shipmentEventFact.location = shipmentEvent.location;
			shipmentEventFact.trackingNumber = shipmentEvent.trackingNumber;
			shipmentEventFact.shippingStatus = shipmentEvent.shippingStatus;
			shipmentEventFact.shippedDate = shipmentEvent.shippedDate;
			shipmentEventFact.expectedDeliveryDate = shipmentEvent.expectedDeliveryDate;
			shipmentEventFact.actualDeliveryDate = shipmentEvent.actualDeliveryDate;
			shipmentEventFact.shippingCost = shipmentEvent.shippingCost;

			return shipmentEventFact;
		}
	}

	@Data
	public static class ShipmentEventFact {
		public String shipmentEventSk;
		public String orderSk;
		public String carrierServiceSk;
		public Integer shipmentId;
		public Integer shipmentEventId;
		public String status;
		public String location;
		public String trackingNumber;
		public String shippingStatus;
		public Date shippedDate;
		public Date expectedDeliveryDate;
		public Date actualDeliveryDate;
		public Double shippingCost;
	}

	public static class ShipmentEvent extends DebeziumSourceRecord {
		public Integer shipmentId;
		public Integer orderId;
		public Integer carrierId;
		public Integer serviceId;
		public Integer shipmentEventId;
		public String status;
		public String location;
		public String trackingNumber;
		public String shippingStatus;
		public Date shippedDate;
		public Date expectedDeliveryDate;
		public Date actualDeliveryDate;
		public Double shippingCost;

		public OrderDimension order;
		public CarrierServiceDimension carrierService;
	}

	public static class Shipment extends DebeziumSourceRecord {
		public Integer shipmentId;
		public Integer orderId;
		public Integer carrierId;
		public Integer serviceId;
		public String trackingNumber;
		public String shippingStatus;
		public Date shippedDate;
		public Date expectedDeliveryDate;
		public Date actualDeliveryDate;
		public Double shippingCost;
	}

	public static class ShipmentJsonParser implements MapFunction<String, Shipment> {
		private final ObjectMapper objectMapper = new ObjectMapper();

		@Override
		public Shipment map(String jsonString) throws Exception {
			JsonNode node = objectMapper.readTree(jsonString);
			Shipment shipment = new Shipment();

			shipment.shipmentId = node.get("shipment_id").asInt();
			shipment.orderId = node.get("order_id").asInt();
			shipment.carrierId = node.get("carrier_id").asInt();
			shipment.serviceId = node.get("service_id").asInt();
			shipment.trackingNumber = node.get("tracking_number").asText();
			shipment.shippingStatus = node.get("shipping_status").asText();
			shipment.shippedDate = Date.valueOf(LocalDate.parse(node.get("shipped_date").asText()));
			shipment.expectedDeliveryDate = Date.valueOf(LocalDate.parse(node.get("expected_delivery_date").asText()));
			JsonNode actualDeliveryDate = node.get("actual_delivery_date");
			if (actualDeliveryDate.asText() != "null")
				shipment.actualDeliveryDate = Date.valueOf(LocalDate.parse(actualDeliveryDate.asText()));
			shipment.shippingCost = CommonFunctions.base64ToScaledDouble(node.get("shipping_cost").asText(), 2);

			shipment.op = node.get("op").asText();
			shipment.tsMs = node.get("ts_ms").asLong();

			return shipment;
		}
	}

	public static class ShipmentEventJsonParser implements MapFunction<String, ShipmentEvent> {
		private final ObjectMapper objectMapper = new ObjectMapper();

		@Override
		public ShipmentEvent map(String jsonString) throws Exception {
			JsonNode node = objectMapper.readTree(jsonString);
			ShipmentEvent shipmentEvent = new ShipmentEvent();

			shipmentEvent.shipmentEventId = node.get("event_id").asInt();
			shipmentEvent.shipmentId = node.get("shipment_id").asInt();
			shipmentEvent.status = node.get("status").asText();
			shipmentEvent.location = node.get("location").asText();

			shipmentEvent.op = node.get("op").asText();
			shipmentEvent.tsMs = node.get("ts_ms").asLong();

			return shipmentEvent;
		}
	}

	public static class ShipmentsJoinFunction
			extends KeyedCoProcessFunction<Integer, ShipmentEvent, Shipment, ShipmentEvent> {
		private transient MapState<Integer, ShipmentEvent> latestShipmentEventState;
		private transient ValueState<Shipment> latestShipmentState;

		@Override
		public void open(OpenContext ctx) throws Exception {
			latestShipmentEventState = getRuntimeContext().getMapState(new MapStateDescriptor<>("latest",
					TypeInformation.of(Integer.class), TypeInformation.of(ShipmentEvent.class)));
			latestShipmentState = getRuntimeContext()
					.getState(new ValueStateDescriptor<>("latestCarrier", TypeInformation.of(Shipment.class)));
		}

		@Override
		public void processElement1(ShipmentEvent service, Context context, Collector<ShipmentEvent> out)
				throws Exception {
			latestShipmentEventState.put(service.serviceId, service);

			Shipment currentCarrier = latestShipmentState.value();
			if (currentCarrier != null) {
				out.collect(createJoinedDimension(service, currentCarrier));
			}
		}

		@Override
		public void processElement2(Shipment carrier, Context context, Collector<ShipmentEvent> out) throws Exception {
			latestShipmentState.update(carrier);

			Iterable<Map.Entry<Integer, ShipmentEvent>> services = latestShipmentEventState.entries();
			if (services != null) {
				for (Map.Entry<Integer, ShipmentEvent> entry : services) {
					ShipmentEvent currentBook = entry.getValue();
					out.collect(createJoinedDimension(currentBook, carrier));
				}
			}
		}

		private ShipmentEvent createJoinedDimension(ShipmentEvent shipmentEvent, Shipment shipment) {
			shipmentEvent.shipmentId = shipment.shipmentId;
			shipmentEvent.orderId = shipment.orderId;
			shipmentEvent.carrierId = shipment.carrierId;
			shipmentEvent.serviceId = shipment.serviceId;
			shipmentEvent.trackingNumber = shipment.trackingNumber;
			shipmentEvent.shippingStatus = shipment.shippingStatus;
			shipmentEvent.shippedDate = shipment.shippedDate;
			shipmentEvent.expectedDeliveryDate = shipment.expectedDeliveryDate;
			shipmentEvent.actualDeliveryDate = shipment.actualDeliveryDate;
			shipmentEvent.shippingCost = shipment.shippingCost;
			return shipmentEvent;
		}
	}

	public static void main(String[] args) throws Exception {
		String groupId = System.getenv("GROUP_ID");
		String shipmentsSourceTopic = ShipmentsLandingJob.sinkTopic;
		String shipmentEventSourceTopic = ShipmentEventsLandingJob.sinkTopic;
		String sinkTopic = TopicNameBuilder.build("facts.shipment_events");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Order Dimension
		String orderDimensionTopic = OrdersDimensionJob.sinkTopic;
		KafkaSource<OrderDimension> orderDimensionSource = KafkaSource.<OrderDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(orderDimensionTopic)
				.setGroupId(groupId).setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new PojoDeserializer<OrderDimension>(OrderDimension.class)).build();
		DataStream<OrderDimension> orderDimensionStream = env.fromSource(orderDimensionSource,
				WatermarkStrategy.<OrderDimension>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((order, timestamp) -> order.validFrom),
				"Orders Source");

		// Carrier Service Dimension
		String carrierServiceTopic = CarrierServiceDimensionJob.sinkTopic;
		KafkaSource<CarrierServiceDimension> carrierServiceDimensionSource = KafkaSource
				.<CarrierServiceDimension>builder().setBootstrapServers(KafkaProperties.bootStrapServers)
				.setTopics(carrierServiceTopic).setGroupId(groupId).setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new PojoDeserializer<CarrierServiceDimension>(CarrierServiceDimension.class))
				.build();
		DataStream<CarrierServiceDimension> carrierServiceDimensionStream = env.fromSource(
				carrierServiceDimensionSource,
				WatermarkStrategy.<CarrierServiceDimension>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((carrierService, timestamp) -> carrierService.validFrom),
				"Carrier Service Source");

		// Shipment Events
		KafkaSource<String> shipmentEventsSource = KafkaSource.<String>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setTopics(shipmentEventSourceTopic).setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> shippingEventsRawStream = env.fromSource(shipmentEventsSource,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((orderItem, timestamp) -> timestamp),
				"Shipment Event source");

		DataStream<ShipmentEvent> shipmentEventsStream = shippingEventsRawStream.map(new ShipmentEventJsonParser())
				.name("Parse JSON to Shipping Event")
				.keyBy(shipmentEvent -> shipmentEvent.shipmentId);

		// Shipments
		KafkaSource<String> shipmentSource = KafkaSource.<String>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setTopics(shipmentsSourceTopic).setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> shipmentRawStream = env.fromSource(shipmentSource,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((orderItem, timestamp) -> timestamp),
				"Shipment Source");

		DataStream<Shipment> shipmentsStream = shipmentRawStream.map(new ShipmentJsonParser())
				.name("Parse JSON to Shipment")
				.keyBy(shipment -> shipment.shipmentId);

		// Join Shipments to Shipment events
		DataStream<ShipmentEvent> shipmentEventsJoinedStream = shipmentEventsStream.connect(shipmentsStream)
				.process(new ShipmentsJoinFunction());

		// Enrich order item facts
		DataStream<ShipmentEvent> shipmentEventsWithOrders = shipmentEventsJoinedStream
				.keyBy(shipmentEvent -> shipmentEvent.orderId)
				.intervalJoin(orderDimensionStream.keyBy(order -> order.orderId))
				.between(Duration.ofDays(-365 * 100), Duration.ofMillis(100)).process(
						new ProcessJoinFunction<ShipmentEvent, OrderDimension, ShipmentEvent>() {
							@Override
							public void processElement(ShipmentEvent left, OrderDimension right, Context ctx,
									Collector<ShipmentEvent> out) {
								if (left.tsMs >= right.validFrom & left.tsMs < right.validTo) {
									left.order = right;
									out.collect(left);
								}
							}
						});

		DataStream<ShipmentEvent> shipmentEventsWithServices = shipmentEventsWithOrders
				.keyBy(shipmentEvent -> shipmentEvent.serviceId)
				.intervalJoin(carrierServiceDimensionStream.keyBy(service -> service.serviceId))
				.between(Duration.ofDays(-365 * 100), Duration.ofMillis(0))
				.process(new ProcessJoinFunction<ShipmentEvent, CarrierServiceDimension, ShipmentEvent>() {
					@Override
					public void processElement(ShipmentEvent left, CarrierServiceDimension right, Context ctx,
							Collector<ShipmentEvent> out) {
						if (left.tsMs >= right.validFrom & left.tsMs < right.validTo) {
							left.carrierService = right;
							out.collect(left);
						}
					}
				});

		DataStream<ShipmentEventFact> orderFacts = shipmentEventsWithServices.map(new ShipmentEventFactMapping());

		KafkaSink<ShipmentEventFact> streamSink = KafkaSink.<ShipmentEventFact>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(sinkTopic)
						.setValueSerializationSchema(new PojoSerializer<ShipmentEventFact>()).build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

		JdbcStatementBuilder<ShipmentEventFact> sinkStatement = (statement, shipmentEvent) -> {
			statement.setString(1, shipmentEvent.shipmentEventSk);
			statement.setString(2, shipmentEvent.orderSk);
			statement.setString(3, shipmentEvent.carrierServiceSk);
			statement.setInt(4, shipmentEvent.shipmentId);
			statement.setInt(5, shipmentEvent.shipmentEventId);
			statement.setString(6, shipmentEvent.status);
			statement.setString(7, shipmentEvent.location);
			statement.setString(8, shipmentEvent.trackingNumber);
			statement.setString(9, shipmentEvent.shippingStatus);
			statement.setDate(10, shipmentEvent.shippedDate);
			statement.setDate(11, shipmentEvent.expectedDeliveryDate);
			statement.setDate(12, shipmentEvent.actualDeliveryDate);
			statement.setDouble(13, shipmentEvent.shippingCost);
		};

		JdbcSink<ShipmentEventFact> jdbcSink = JdbcSink.<ShipmentEventFact>builder().withExecutionOptions(
				JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(200).withMaxRetries(5).build())
				.withQueryStatement("""
							INSERT INTO modeling_db.f_shipment_events (
							    shipmentEventSk,
								orderSk,
								carrierServiceSk,
								shipmentId,
								shipmentEventId,
								status,
								location,
								trackingNumber,
								shippingStatus,
								shippedDate,
								expectedDeliveryDate,
								actualDeliveryDate,
								shippingCost
						) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
							""", sinkStatement).buildAtLeastOnce(DWConnectionCommonOptions.commonOptions);

		orderFacts.sinkTo(jdbcSink);
		orderFacts.sinkTo(streamSink);

		env.execute("f_shipment_events job");
	}
}
