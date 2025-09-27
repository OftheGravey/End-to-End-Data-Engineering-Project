package com.extractor.flink.jobs.facts;

import java.time.Duration;
import java.util.function.BiFunction;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.jdbc.core.datastream.sink.JdbcSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.jobs.dimensions.CarrierServiceDimensionJob;
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob;
import com.extractor.flink.jobs.landing.ShipmentEventsLandingJob;
import com.extractor.flink.jobs.landing.ShipmentsLandingJob;
import com.extractor.flink.join_functions.ShipmentsJoinFunction;
import com.extractor.flink.model.dimensions.CarrierServiceDimension;
import com.extractor.flink.model.dimensions.OrderDimension;
import com.extractor.flink.model.dimensions.TargetDimensionRecord;
import com.extractor.flink.model.fact.ShipmentEventFact;
import com.extractor.flink.model.joined.ShipmentEventJoined;
import com.extractor.flink.model.source.Shipment;
import com.extractor.flink.model.source.ShipmentEvent;
import com.extractor.flink.utils.DWConnectionCommonOptions;
import com.extractor.flink.utils.TopicNameBuilder;

import com.extractor.flink.functions.PojoDeserializer;
import com.extractor.flink.functions.PojoSerializer;

public class ShipmentEventsFactJob {
	private static final String groupId = System.getenv("GROUP_ID");
	private static final ObjectMapper mapper = new ObjectMapper();

	public static DataStream<OrderDimension> orderDimensionStreamInput(StreamExecutionEnvironment env) {
		String orderDimensionTopic = OrdersDimensionJob.sinkTopic;
		KafkaSource<OrderDimension> orderDimensionSource = KafkaSource.<OrderDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(orderDimensionTopic)
				.setGroupId(groupId).setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new PojoDeserializer<OrderDimension>(OrderDimension.class)).build();
		DataStream<OrderDimension> orderDimensionStream = env.fromSource(orderDimensionSource,
				WatermarkStrategy.<OrderDimension>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((order, timestamp) -> order.validFrom),
				"Orders Source");
		return orderDimensionStream;
	}

	public static DataStream<CarrierServiceDimension> carrierServiceDimensionStreamInput(
			StreamExecutionEnvironment env) {
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
		return carrierServiceDimensionStream;
	}

	public static DataStream<ShipmentEvent> shippingEventStreamInput(StreamExecutionEnvironment env) {
		String shipmentEventSourceTopic = ShipmentEventsLandingJob.sinkTopic;
		KafkaSource<String> shipmentEventsSource = KafkaSource.<String>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setTopics(shipmentEventSourceTopic).setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> shippingEventsRawStream = env.fromSource(shipmentEventsSource,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((orderItem, timestamp) -> timestamp),
				"Shipment Event source");

		DataStream<ShipmentEvent> shipmentEventsStream = shippingEventsRawStream
				.map((MapFunction<String, ShipmentEvent>) value -> mapper.readValue(value, ShipmentEvent.class))
				.name("Parse JSON to Shipping Event")
				.keyBy(shipmentEvent -> shipmentEvent.shipmentId);
		return shipmentEventsStream;
	}

	public static DataStream<Shipment> shipmentStreamInput(StreamExecutionEnvironment env) {
		String shipmentsSourceTopic = ShipmentsLandingJob.sinkTopic;
		KafkaSource<String> shipmentSource = KafkaSource.<String>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setTopics(shipmentsSourceTopic).setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> shipmentRawStream = env.fromSource(shipmentSource,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((orderItem, timestamp) -> timestamp),
				"Shipment Source");

		DataStream<Shipment> shipmentsStream = shipmentRawStream
				.map((MapFunction<String, Shipment>) value -> mapper.readValue(value, Shipment.class))
				.name("Parse JSON to Shipment")
				.keyBy(shipment -> shipment.shipmentId);
		return shipmentsStream;
	}

	public static class ShipmentEventFactDimensionJoin<T extends TargetDimensionRecord>
			extends ProcessJoinFunction<ShipmentEventJoined, T, ShipmentEventJoined> {
		private final BiFunction<ShipmentEventJoined, T, ShipmentEventJoined> joinLogic;

		public ShipmentEventFactDimensionJoin(BiFunction<ShipmentEventJoined, T, ShipmentEventJoined> joinLogic) {
			this.joinLogic = joinLogic;
		}

		@Override
		public void processElement(ShipmentEventJoined left, T right, Context ctx,
				Collector<ShipmentEventJoined> out) {
			if (left.tsMs >= right.validFrom & left.tsMs < right.validTo) {
				ShipmentEventJoined result = joinLogic.apply(left, right);
				out.collect(result);
			}
		}
	}

	public static DataStream<ShipmentEventJoined> joinDimensionsToFacts(
			DataStream<ShipmentEventJoined> shipmentEventsStream, DataStream<OrderDimension> orderDimeDataStream,
			DataStream<CarrierServiceDimension> carrierServiceStream) {
		BiFunction<ShipmentEventJoined, OrderDimension, ShipmentEventJoined> orderJoinFunction = (shipment, order) -> {
			shipment.order = order;
			return shipment;
		};
		DataStream<ShipmentEventJoined> shipmentEventsWithOrders = shipmentEventsStream
				.keyBy(shipmentEvent -> shipmentEvent.shipment.orderId)
				.intervalJoin(orderDimeDataStream.keyBy(order -> order.orderId))
				.between(Duration.ofDays(-365 * 100), Duration.ofMillis(100)).process(
						new ShipmentEventFactDimensionJoin<OrderDimension>(orderJoinFunction));

		BiFunction<ShipmentEventJoined, CarrierServiceDimension, ShipmentEventJoined> carrierJoinFunction = (shipment,
				carrier) -> {
			shipment.carrierService = carrier;
			return shipment;
		};
		DataStream<ShipmentEventJoined> shipmentEventsWithServices = shipmentEventsWithOrders
				.keyBy(shipmentEvent -> shipmentEvent.shipment.serviceId)
				.intervalJoin(carrierServiceStream.keyBy(service -> service.serviceId))
				.between(Duration.ofDays(-365 * 100), Duration.ofMillis(0))
				.process(new ShipmentEventFactDimensionJoin<CarrierServiceDimension>(carrierJoinFunction));

		return shipmentEventsWithServices;
	}

	public static void sinkToKafka(DataStream<ShipmentEventFact> stream) {
		String sinkTopic = TopicNameBuilder.build("facts.shipment_events");
		KafkaSink<ShipmentEventFact> streamSink = KafkaSink.<ShipmentEventFact>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(sinkTopic)
						.setValueSerializationSchema(new PojoSerializer<ShipmentEventFact>()).build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
		stream.sinkTo(streamSink);
	}

	public static void sinkIntoDB(DataStream<ShipmentEventFact> stream) {

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

		stream.sinkTo(jdbcSink);
	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<OrderDimension> orderDimensionStream = orderDimensionStreamInput(env);
		DataStream<CarrierServiceDimension> carrierServiceDimensionStream = carrierServiceDimensionStreamInput(env);

		DataStream<ShipmentEvent> shipmentEventsStream = shippingEventStreamInput(env);
		DataStream<Shipment> shipmentsStream = shipmentStreamInput(env);

		// Join Shipments to Shipment events
		DataStream<ShipmentEventJoined> shipmentEventsJoinedStream = shipmentEventsStream.connect(shipmentsStream)
				.process(new ShipmentsJoinFunction());

		// Enrich order item facts
		DataStream<ShipmentEventJoined> joinedStream = joinDimensionsToFacts(shipmentEventsJoinedStream,
				orderDimensionStream, carrierServiceDimensionStream);

		DataStream<ShipmentEventFact> orderFacts = joinedStream.map(new ShipmentEventFact.ShipmentEventFactMapping());

		sinkToKafka(orderFacts);
		sinkIntoDB(orderFacts);

		env.execute("f_shipment_events job");
	}
}
