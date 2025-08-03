package com.extractor.flink.jobs.facts;

import java.time.Duration;
import java.util.UUID;

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
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

import com.extractor.flink.functions.CommonFunctions;
import com.extractor.flink.functions.DebeziumSourceRecord;
import com.extractor.flink.functions.KafkaProperties;
import com.extractor.flink.jobs.dimensions.BooksDimensionJob;
import com.extractor.flink.jobs.dimensions.CustomersDimensionJob;
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob;
import com.extractor.flink.jobs.dimensions.BooksDimensionJob.BookDimension;
import com.extractor.flink.jobs.dimensions.CustomersDimensionJob.CustomerDimension;
import com.extractor.flink.jobs.dimensions.OrdersDimensionJob.OrderDimension;
import com.extractor.flink.jobs.landing.OrderItemsLandingJob;
import com.extractor.flink.utils.DWConnectionCommonOptions;
import com.extractor.flink.utils.TopicNameBuilder;

import lombok.Data;

import com.extractor.flink.functions.PojoDeserializer;
import com.extractor.flink.functions.PojoSerializer;

public class OrderItemsFactJob {
	public static class OrderItemFactMapping implements MapFunction<OrderItem, OrderItemFact> {
		@Override
		public OrderItemFact map(OrderItem orderItem) {
			OrderItemFact orderItemFact = new OrderItemFact();
			orderItemFact.orderItemSk = UUID.randomUUID().toString();
			orderItemFact.orderSk = orderItem.order.orderSk;
			orderItemFact.bookSk = orderItem.book.bookSk;
			orderItemFact.customerSk = orderItem.customer.customerSk;
			orderItemFact.orderItemId = orderItem.orderItemId;
			orderItemFact.quantity = orderItem.quantity;
			orderItemFact.priceAtPurchase = orderItem.priceAtPurchase;
			orderItemFact.discount = orderItem.discount;
			orderItemFact.transactionTime = orderItem.tsMs;
			orderItemFact.priceTotal = orderItem.quantity * orderItem.priceAtPurchase * (1 - orderItem.discount);
			return orderItemFact;
		}
	}

	@Data
	public static class OrderItemFact {
		public String orderItemSk;
		public String orderSk;
		public String bookSk;
		public String customerSk;
		public Integer orderItemId;
		public Integer quantity;
		public Double priceAtPurchase;
		public Double discount;
		public Long transactionTime;
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
		public OrderDimension order;
		public CustomerDimension customer;
		public BookDimension book;
	}

	public static class OrderItemJsonParser implements MapFunction<String, OrderItem> {
		private final ObjectMapper objectMapper = new ObjectMapper();

		@Override
		public OrderItem map(String jsonString) throws Exception {
			JsonNode node = objectMapper.readTree(jsonString);
			OrderItem orderItem = new OrderItem();

			orderItem.orderItemId = node.get("order_item_id").asInt();
			orderItem.orderId = node.get("order_id").asInt();
			orderItem.bookId = node.get("book_id").asInt();
			orderItem.quantity = node.get("quantity").asInt();
			orderItem.priceAtPurchase = CommonFunctions.base64ToScaledDouble(node.get("price_at_purchase").asText(), 2);
			orderItem.discount = CommonFunctions.base64ToScaledDouble(node.get("discount").asText(), 2);

			orderItem.op = node.get("op").asText();
			orderItem.emittedTsMs = node.get("emitted_ts_ms").asLong();
			orderItem.tsMs = node.get("ts_ms").asLong();
			orderItem.connectorVersion = node.get("connector_version").asText();
			orderItem.transactionId = node.get("transaction_id").asText();
			orderItem.lsn = node.get("lsn").asLong();

			return orderItem;
		}
	}

	public static void main(String[] args) throws Exception {
		String groupId = System.getenv("GROUP_ID");
		String sourceTopic = OrderItemsLandingJob.sinkTopic;
		String sinkTopic = TopicNameBuilder.build("facts.order_items");

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

		// Customer Dimension
		String customerDimensionTopic = CustomersDimensionJob.sinkTopic;
		KafkaSource<CustomerDimension> customerDimensionSource = KafkaSource.<CustomerDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(customerDimensionTopic)
				.setGroupId(groupId).setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new PojoDeserializer<CustomerDimension>(CustomerDimension.class)).build();
		DataStream<CustomerDimension> customerDimensionStream = env.fromSource(customerDimensionSource,
				WatermarkStrategy.<CustomerDimension>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((customer, timestamp) -> customer.validFrom),
				"Customer Source");

		// Book Dimension
		String bookDimensionTopic = BooksDimensionJob.sinkTopic;
		KafkaSource<BookDimension> bookDimensionSource = KafkaSource.<BookDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(bookDimensionTopic).setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new PojoDeserializer<BookDimension>(BookDimension.class)).build();
		DataStream<BookDimension> bookDimensionStream = env.fromSource(bookDimensionSource,
				WatermarkStrategy.<BookDimension>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((book, timestamp) -> book.validFrom),
				"Book Source");

		// Order item from Kafka
		KafkaSource<String> source = KafkaSource.<String>builder().setBootstrapServers(KafkaProperties.bootStrapServers)
				.setTopics(sourceTopic).setGroupId(groupId).setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new SimpleStringSchema()).build();

		DataStream<String> orderItemRawStream = env.fromSource(source,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((orderItem, timestamp) -> timestamp),
				"Order item source");

		DataStream<OrderItem> orderItemStream = orderItemRawStream.map(new OrderItemJsonParser())
				.name("Parse JSON to Order Item");

		// Enrich order item facts
		DataStream<OrderItem> orderItemWithOrders = orderItemStream.keyBy(order -> order.orderId)
				.intervalJoin(orderDimensionStream.keyBy(order -> order.orderId))
				.between(Duration.ofDays(-365 * 100), Duration.ofMillis(100)).process(
						new ProcessJoinFunction<OrderItemsFactJob.OrderItem, OrdersDimensionJob.OrderDimension, OrderItem>() {
							@Override
							public void processElement(OrderItem left, OrderDimension right, Context ctx,
									Collector<OrderItem> out) {
								if (left.tsMs >= right.validFrom & left.tsMs < right.validTo) {
									left.order = right;
									out.collect(left);
								}
							}
						});

		DataStream<OrderItem> orderItemWithCustomers = orderItemWithOrders
				.keyBy(orderItem -> orderItem.order.customerId)
				.intervalJoin(customerDimensionStream.keyBy(customer -> customer.customerId))
				.between(Duration.ofDays(-365 * 100), Duration.ofMillis(0))
				.process(new ProcessJoinFunction<OrderItem, CustomerDimension, OrderItem>() {
					@Override
					public void processElement(OrderItem left, CustomerDimension right, Context ctx,
							Collector<OrderItem> out) {
						if (left.tsMs >= right.validFrom & left.tsMs < right.validTo) {
							left.customer = right;
							out.collect(left);
						}
					}
				});

		DataStream<OrderItem> orderItemWithBooks = orderItemWithCustomers.keyBy(book -> book.bookId)
				.intervalJoin(bookDimensionStream.keyBy(book -> book.bookId))
				.between(Duration.ofDays(-365 * 100), Duration.ofMillis(0))
				.process(new ProcessJoinFunction<OrderItem, BookDimension, OrderItem>() {
					@Override
					public void processElement(OrderItem left, BookDimension right, Context ctx,
							Collector<OrderItem> out) {
						if (left.tsMs >= right.validFrom & left.tsMs < right.validTo) {
							left.book = right;
							out.collect(left);
						}
					}
				});

		DataStream<OrderItemFact> orderFacts = orderItemWithBooks.map(new OrderItemFactMapping());

		KafkaSink<OrderItemFact> streamSink = KafkaSink.<OrderItemFact>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(sinkTopic)
						.setValueSerializationSchema(new PojoSerializer<OrderItemFact>()).build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();

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

		orderFacts.sinkTo(jdbcSink);
		orderFacts.sinkTo(streamSink);

		env.execute("f_order_items job");
	}
}
