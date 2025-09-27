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
import com.extractor.flink.jobs.landing.OrdersLandingJob;
import com.extractor.flink.model.dimensions.OrderDimension;
import com.extractor.flink.model.source.Order;
import com.extractor.flink.utils.DWConnectionCommonOptions;
import com.extractor.flink.utils.TopicNameBuilder;

public class OrdersDimensionJob {
	public static String sinkTopic = TopicNameBuilder.build("dimensions.orders");
	static String groupId = System.getenv("GROUP_ID");
	private static final ObjectMapper mapper = new ObjectMapper();

	public static class OrdersSCD2ProcessFunction extends SCD2ProcessFunction<Order, OrderDimension> {
		public OrdersSCD2ProcessFunction() {
			super(TypeInformation.of(Order.class), TypeInformation.of(OrderDimension.class), OrderDimension::new);
		}
	}

	public static class OrdersSCD2MostValidFunction extends SCD2MostValidFunction<OrderDimension> {
		public OrdersSCD2MostValidFunction() {
			super(TypeInformation.of(OrderDimension.class));
		}

	}

	public static DataStream<Order> orderStreamInput(StreamExecutionEnvironment env) {
		String sourceTopic = OrdersLandingJob.sinkTopic;
		KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(KafkaProperties.bootStrapServers)
				.setTopics(sourceTopic).setGroupId(groupId).setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema()).build();

		DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Orders Source");

		DataStream<Order> orderStream = rawStream
				.map((MapFunction<String, Order>) value -> mapper.readValue(value, Order.class))
				.name("Parse JSON to Order");

		return orderStream;
	}

	public static DataStream<OrderDimension> dimensionTransformation(DataStream<Order> orderStream) {
				DataStream<OrderDimension> scd2Stream = orderStream.keyBy(record -> record.orderId)
				.process(new OrdersSCD2ProcessFunction()).name("SCD2 Transformation");
		DataStream<OrderDimension> scd2StreamWatermarked = scd2Stream.assignTimestampsAndWatermarks(
				WatermarkStrategy.<OrderDimension>forBoundedOutOfOrderness(Duration.ofSeconds(10))
						.withTimestampAssigner((record, timestamp) -> record.validFrom));


				
		DataStream<OrderDimension> orderStreamConsolidated = scd2StreamWatermarked.keyBy(record -> record.orderSk)
				.process(new OrdersSCD2MostValidFunction()).name("SCD2 Consolidation");
		

		return orderStreamConsolidated;
	}

	public static void sinkIntoKafka(DataStream<OrderDimension> stream) {
		KafkaSink<OrderDimension> streamSink = KafkaSink.<OrderDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(sinkTopic)
						.setValueSerializationSchema(new PojoSerializer<OrderDimension>()).build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
	
		stream.sinkTo(streamSink);
	}

	public static void sinkIntoDB(DataStream<OrderDimension> stream) {
		JdbcStatementBuilder<OrderDimension> sinkStatement = (statement, order) -> {
			statement.setInt(1, order.orderId);
			statement.setString(2, order.status);
			statement.setString(3, order.shippingMethod);
			statement.setDate(4, order.orderDate);
			statement.setString(5, order.orderSk);
			statement.setLong(6, order.validFrom);
			statement.setLong(7, order.validTo);
		};
	
		JdbcSink<OrderDimension> jdbcSink = JdbcSink.<OrderDimension>builder().withExecutionOptions(
				JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(200).withMaxRetries(5).build())
				.withQueryStatement("""
						insert into modeling_db.d_orders
						(
						orderId,
						status,
						shippingMethod,
						orderDate,
						orderSk,
						validFrom,
						validTo
						)
						values (?, ?, ?, ?, ?, ?, ?)
						ON CONFLICT (ordersk) DO UPDATE SET
						validTo = EXCLUDED.validTo
						""", sinkStatement).buildAtLeastOnce(DWConnectionCommonOptions.commonOptions);
	
		stream.sinkTo(jdbcSink);
	}
	
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<Order> orderStream = orderStreamInput(env);
		
		DataStream<OrderDimension> resultStream = dimensionTransformation(orderStream);
		
		sinkIntoDB(resultStream);
		sinkIntoKafka(resultStream);

		env.execute("d_orders job");
	}
}
