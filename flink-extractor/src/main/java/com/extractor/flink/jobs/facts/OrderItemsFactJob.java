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
import com.extractor.flink.jobs.dimensions.BooksDimensionJob;
import com.extractor.flink.jobs.dimensions.CustomersDimensionJob;
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob;
import com.extractor.flink.jobs.landing.OrderItemsLandingJob;
import com.extractor.flink.model.dimensions.BookDimension;
import com.extractor.flink.model.dimensions.CustomerDimension;
import com.extractor.flink.model.dimensions.OrderDimension;
import com.extractor.flink.model.dimensions.TargetDimensionRecord;
import com.extractor.flink.model.fact.OrderItemFact;
import com.extractor.flink.model.source.OrderItem;
import com.extractor.flink.utils.DWConnectionCommonOptions;
import com.extractor.flink.utils.TopicNameBuilder;
import com.extractor.flink.functions.PojoDeserializer;
import com.extractor.flink.functions.PojoSerializer;

public class OrderItemsFactJob {
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

	public static DataStream<CustomerDimension> customerDimensionStreamInput(StreamExecutionEnvironment env) {
		String customerDimensionTopic = CustomersDimensionJob.sinkTopic;
		KafkaSource<CustomerDimension> customerDimensionSource = KafkaSource.<CustomerDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(customerDimensionTopic)
				.setGroupId(groupId).setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new PojoDeserializer<CustomerDimension>(CustomerDimension.class)).build();
		DataStream<CustomerDimension> customerDimensionStream = env.fromSource(customerDimensionSource,
				WatermarkStrategy.<CustomerDimension>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((customer, timestamp) -> customer.validFrom),
				"Customer Source");
		return customerDimensionStream;
	}

	public static DataStream<BookDimension> bookDimensionStreamInput(StreamExecutionEnvironment env) {
		String bookDimensionTopic = BooksDimensionJob.sinkTopic;
		KafkaSource<BookDimension> bookDimensionSource = KafkaSource.<BookDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(bookDimensionTopic).setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new PojoDeserializer<BookDimension>(BookDimension.class)).build();
		DataStream<BookDimension> bookDimensionStream = env.fromSource(bookDimensionSource,
				WatermarkStrategy.<BookDimension>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((book, timestamp) -> book.validFrom),
				"Book Source");

		return bookDimensionStream;
	}

	public static DataStream<OrderItem> orderItemStreamInput(StreamExecutionEnvironment env) {
		String sourceTopic = OrderItemsLandingJob.sinkTopic;
		KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(KafkaProperties.bootStrapServers)
				.setTopics(sourceTopic).setGroupId(groupId).setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema()).build();

		DataStream<String> orderItemRawStream = env.fromSource(source,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((orderItem, timestamp) -> timestamp),
				"Order item source");

		DataStream<OrderItem> orderItemStream = orderItemRawStream
				.map((MapFunction<String, OrderItem>) value -> mapper.readValue(value, OrderItem.class))
				.name("Parse JSON to Order Item");

		return orderItemStream;
	}

	public static class OrderItemFactDimensionJoin<T extends TargetDimensionRecord>
			extends ProcessJoinFunction<OrderItem, T, OrderItem> {
		private final BiFunction<OrderItem, T, OrderItem> joinLogic;

		public OrderItemFactDimensionJoin(BiFunction<OrderItem, T, OrderItem> joinLogic) {
			this.joinLogic = joinLogic;
		}

		@Override
		public void processElement(OrderItem left, T right, Context ctx,
				Collector<OrderItem> out) {
			if (left.tsMs >= right.validFrom & left.tsMs < right.validTo) {
				OrderItem result = joinLogic.apply(left, right);
				out.collect(result);
			}
		}
	}

	public static DataStream<OrderItem> joinDimensionsToFacts(DataStream<OrderItem> orderItemStream,
			DataStream<OrderDimension> orderDimensionStream, DataStream<CustomerDimension> customerDimensionStream,
			DataStream<BookDimension> bookDimensionStream) {
		// Enrich order item facts
		BiFunction<OrderItem, OrderDimension, OrderItem> orderJoinFunction = (orderItem, order) -> {
			orderItem.order = order;
			return orderItem;
		};
		DataStream<OrderItem> orderItemWithOrders = orderItemStream.keyBy(order -> order.orderId)
				.intervalJoin(orderDimensionStream.keyBy(order -> order.orderId))
				.between(Duration.ofDays(-365 * 100), Duration.ofMillis(100)).process(
						new OrderItemFactDimensionJoin<OrderDimension>(orderJoinFunction));

		BiFunction<OrderItem, CustomerDimension, OrderItem> customerJoinFunction = (orderItem, customer) -> {
			orderItem.customer = customer;
			return orderItem;
		};
		DataStream<OrderItem> orderItemWithCustomers = orderItemWithOrders
				.keyBy(orderItem -> orderItem.order.customerId)
				.intervalJoin(customerDimensionStream.keyBy(customer -> customer.customerId))
				.between(Duration.ofDays(-365 * 100), Duration.ofMillis(0))
				.process(new OrderItemFactDimensionJoin<CustomerDimension>(customerJoinFunction));

		BiFunction<OrderItem, BookDimension, OrderItem> bookJoinFunction = (orderItem, book) -> {
			orderItem.book = book;
			return orderItem;
		};
		DataStream<OrderItem> orderItemWithBooks = orderItemWithCustomers.keyBy(book -> book.bookId)
				.intervalJoin(bookDimensionStream.keyBy(book -> book.bookId))
				.between(Duration.ofDays(-365 * 100), Duration.ofMillis(0))
				.process(new OrderItemFactDimensionJoin<BookDimension>(bookJoinFunction));
		return orderItemWithBooks;

	}

	private static void sinkIntoKafka(DataStream<OrderItemFact> stream) {
		String sinkTopic = TopicNameBuilder.build("facts.order_items");
		KafkaSink<OrderItemFact> streamSink = KafkaSink.<OrderItemFact>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(sinkTopic)
						.setValueSerializationSchema(new PojoSerializer<OrderItemFact>()).build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
		stream.sinkTo(streamSink);
	}

	public static void sinkIntoDB(DataStream<OrderItemFact> stream) {
		JdbcStatementBuilder<OrderItemFact> sinkStatement = (statement, orderItem) -> {
			statement.setString(1, orderItem.orderItemSk);
			statement.setString(2, orderItem.orderSk);
			statement.setString(3, orderItem.bookSk);
			statement.setString(4, orderItem.customerSk);
			statement.setInt(5, orderItem.orderItemId);
			statement.setDouble(6, orderItem.quantity);
			statement.setDouble(7, orderItem.priceAtPurchase);
			statement.setDouble(8, orderItem.discount);
			statement.setLong(9, orderItem.transactionTime);
			statement.setDouble(10, orderItem.priceTotal);
		};

		JdbcSink<OrderItemFact> jdbcSink = JdbcSink.<OrderItemFact>builder().withExecutionOptions(
				JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(200).withMaxRetries(5).build())
				.withQueryStatement("""
						INSERT INTO modeling_db.f_order_items (
											  orderItemSk,
											  orderSk,
											  bookSk,
											  customerSk,
											  orderItemId,
											  quantity,
											  priceAtPurchase,
											  discount,
											  transactionTime,
											  priceTotal
										  ) VALUES (?,?,?,?,?,?,?,?,?,?)
						ON CONFLICT DO NOTHING
						""", sinkStatement).buildAtLeastOnce(DWConnectionCommonOptions.commonOptions);

		stream.sinkTo(jdbcSink);
	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Order Dimension
		DataStream<OrderDimension> orderDimensionStream = orderDimensionStreamInput(env);
		DataStream<CustomerDimension> customerDimensionStream = customerDimensionStreamInput(env);
		DataStream<BookDimension> bookDimensionStream = bookDimensionStreamInput(env);

		// Order item from Kafka
		DataStream<OrderItem> orderItemStream = orderItemStreamInput(env);

		DataStream<OrderItem> joinedStream = joinDimensionsToFacts(orderItemStream, orderDimensionStream,
				customerDimensionStream, bookDimensionStream);

		DataStream<OrderItemFact> orderFacts = joinedStream.map(new OrderItemFact.OrderItemFactMapping());

		sinkIntoKafka(orderFacts);
		sinkIntoDB(orderFacts);

		env.execute("f_order_items job");
	}
}
