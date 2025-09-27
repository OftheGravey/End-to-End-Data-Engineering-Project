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
import com.extractor.flink.jobs.landing.AuthorsLandingJob;
import com.extractor.flink.jobs.landing.BooksLandingJob;
import com.extractor.flink.join_functions.BookAuthorJoinFunction;
import com.extractor.flink.model.dimensions.BookDimension;
import com.extractor.flink.model.joined.BookAuthorJoined;
import com.extractor.flink.model.source.Author;
import com.extractor.flink.model.source.Book;
import com.extractor.flink.utils.DWConnectionCommonOptions;
import com.extractor.flink.utils.TopicNameBuilder;

public class BooksDimensionJob {
	static String groupId = System.getenv("GROUP_ID");
	public static String sinkTopic = TopicNameBuilder.build("dimensions.books");
	private static final ObjectMapper mapper = new ObjectMapper();

	public static class BooksSCD2ProcessFunction extends SCD2ProcessFunction<BookAuthorJoined, BookDimension> {
		public BooksSCD2ProcessFunction() {
			super(TypeInformation.of(BookAuthorJoined.class), TypeInformation.of(BookDimension.class),
					BookDimension::new);
		}
	}

	public static class BooksSCD2MostValidFunction extends SCD2MostValidFunction<BookDimension> {
		public BooksSCD2MostValidFunction() {
			super(TypeInformation.of(BookDimension.class));
		}
	}

	public static void sinkIntoDB(DataStream<BookDimension> stream) {
		JdbcStatementBuilder<BookDimension> sinkStatement = (statement, book) -> {
			statement.setString(1, book.bookSk);
			statement.setInt(2, book.bookId);
			statement.setInt(3, book.authorId);
			statement.setString(4, book.title);
			statement.setString(5, book.isbn);
			statement.setDate(6, book.publishedDate);
			statement.setString(7, book.genre);
			statement.setString(8, book.authorFirstName);
			statement.setString(9, book.authorLastName);
			statement.setString(10, book.authorCountry);
			statement.setLong(11, book.validFrom);
			statement.setLong(12, book.validTo);
		};
		JdbcSink<BookDimension> jdbcSink = JdbcSink.<BookDimension>builder().withExecutionOptions(
				JdbcExecutionOptions.builder().withBatchSize(1000).withBatchIntervalMs(200).withMaxRetries(5).build())
				.withQueryStatement("""
						INSERT INTO modeling_db.d_books (
						bookSk, bookId, authorId, title, isbn, publishedDate, genre,
						authorFirstName, authorLastName, authorCountry, validFrom, validTo
						) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
						ON CONFLICT (bookSk) DO UPDATE SET
						validTo = EXCLUDED.validTo
						""", sinkStatement).buildAtLeastOnce(DWConnectionCommonOptions.commonOptions);
		stream.sinkTo(jdbcSink);
	}

	public static void sinkIntoKafka(DataStream<BookDimension> stream) {
		KafkaSink<BookDimension> streamSink = KafkaSink.<BookDimension>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder().setTopic(sinkTopic)
						.setValueSerializationSchema(new PojoSerializer<BookDimension>()).build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
		stream.sinkTo(streamSink);
	}

	public static DataStream<Book> bookStreamInput(StreamExecutionEnvironment env) {
		String bookSourceTopic = BooksLandingJob.sinkTopic;
		KafkaSource<String> bookSource = KafkaSource.<String>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(bookSourceTopic).setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> bookRawStream = env.fromSource(bookSource,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((book, timestamp) -> timestamp),
				"Books Source");

		DataStream<Book> bookStream = bookRawStream
				.map((MapFunction<String, Book>) value -> mapper.readValue(value, Book.class))
				.name("Parse JSON to Book")
				.keyBy(book -> book.authorId);

		return bookStream;
	}

	public static DataStream<Author> authorStreamInput(StreamExecutionEnvironment env) {
		String authorSourceTopic = AuthorsLandingJob.sinkTopic;
				KafkaSource<String> authorSource = KafkaSource.<String>builder()
				.setBootstrapServers(KafkaProperties.bootStrapServers).setTopics(authorSourceTopic).setGroupId(groupId)
				.setStartingOffsets(OffsetsInitializer.earliest()).setValueOnlyDeserializer(new SimpleStringSchema())
				.build();

		DataStream<String> authorRawStream = env.fromSource(authorSource,
				WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
						.withTimestampAssigner((author, timestamp) -> timestamp),
				"Author Source");

		DataStream<Author> authorStream = authorRawStream
				.map((MapFunction<String, Author>) value -> mapper.readValue(value, Author.class))
				.name("Parse JSON to Book")
				.keyBy(author -> author.authorId);

		return authorStream;
	}

	private static DataStream<BookDimension> dimensionTransformation(DataStream<Book> bookStream, DataStream<Author> authorStream) {
		DataStream<BookAuthorJoined> bookAuthorStream = bookStream.connect(authorStream)
				.process(new BookAuthorJoinFunction());

		DataStream<BookDimension> scd2Stream = bookAuthorStream.keyBy(record -> record.book.bookId)
				.process(new BooksSCD2ProcessFunction()).name("SCD2 Transformation");

		DataStream<BookDimension> scd2StreamWatermarked = scd2Stream.assignTimestampsAndWatermarks(
				WatermarkStrategy.<BookDimension>forBoundedOutOfOrderness(Duration.ofSeconds(10))
						.withTimestampAssigner((record, timestamp) -> record.validFrom));

		DataStream<BookDimension> bookStreamConsolidated = scd2StreamWatermarked.keyBy(record -> record.bookSk)
				.process(new BooksSCD2MostValidFunction()).name("Consolidate dimension records");

		return bookStreamConsolidated;

	}

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Books Streams
		DataStream<Book> bookStream = bookStreamInput(env);

		// Author Stream
		DataStream<Author> authorStream = authorStreamInput(env);

		// Join Stream
		DataStream<BookDimension> resultStream = dimensionTransformation(bookStream, authorStream);

		sinkIntoKafka(resultStream);
		sinkIntoDB(resultStream);

		env.execute("d_books job");

	}
}
