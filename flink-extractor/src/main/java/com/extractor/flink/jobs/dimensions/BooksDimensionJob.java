package com.extractor.flink.jobs.dimensions;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Base64;
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
import com.extractor.flink.functions.SCD2ProcessFunction;
import com.extractor.flink.functions.TargetDimensionRecord;
import com.extractor.flink.jobs.landing.AuthorsLandingJob;
import com.extractor.flink.jobs.landing.BooksLandingJob;
import com.extractor.flink.utils.TopicNameBuilder;

public class BooksDimensionJob {
    static String groupId = System.getenv("GROUP_ID");
    public static String sinkTopic = TopicNameBuilder.build("dimensions.books");

    public static class Book extends DebeziumSourceRecord {
        public Integer bookId;
        public String title;
        public Integer authorId;
        public String isbn;
        public Double price;
        public Date publishedDate;
        public String description;
        public String genre;
        public Integer stock;
        public Long emittedTsMs;
        public String connectorVersion;
        public String transactionId;
        public Long lsn;

        // Default constructor
        public Book() {
        }

        @Override
        public String toString() {
            return String.format("Book{bookId=%d, title='%s', op='%s'}",
                    bookId, title, op);
        }
    }

    public static class Author extends DebeziumSourceRecord {
        public Integer authorId;
        public String firstName;
        public String lastName;
        public String biography;
        public String country;

        public Long emittedTsMs;
        public String connectorVersion;
        public String transactionId;
        public Long lsn;

        // Default constructor
        public Author() {
        }

        @Override
        public String toString() {
            return String.format("Book{authorId=%d, firstName='%s', op='%s'}",
                    authorId, firstName, op);
        }
    }

    public static class BookAuthor extends DebeziumSourceRecord {
        // Author
        public Integer authorId;
        public String firstName;
        public String lastName;
        public String biography;
        public String country;
        // Book
        public Integer bookId;
        public String title;
        public String isbn;
        public Double price;
        public Date publishedDate;
        public String description;
        public String genre;
        public Integer stock;

        public Long emittedTsMs;
        public String connectorVersion;
        public String transactionId;
        public Long lsn;

        // Default constructor
        public BookAuthor() {
        }

        @Override
        public String toString() {
            return String.format("Book{authorId=%d, firstName='%s', op='%s'}",
                    authorId, firstName, op);
        }
    }

    public static class BookDimension extends TargetDimensionRecord {
        public Integer bookId;
        public Integer authorId;
        public String bookSk;
        public String title;
        public String isbn;
        public Date publishedDate;
        public String genre;
        public String authorFirstName;
        public String authorLastName;
        public String authorCountry;
        public Timestamp validFrom;
        public Timestamp validTo;

        public BookDimension(BookAuthor record, Long validTo) {
            super(record, validTo);
            this.bookId = record.bookId;
            this.authorId = record.authorId;
            this.title = record.title;
            this.isbn = record.isbn;
            this.publishedDate = record.publishedDate;
            this.genre = record.genre;
            this.authorFirstName = record.firstName;
            this.authorLastName = record.lastName;
            this.authorCountry = record.country;
            this.bookSk = UUID.randomUUID().toString();
            this.validFrom = new Timestamp(record.tsMs);
            this.validTo = new Timestamp(validTo);
        }

        public BookDimension() {};

        @Override
        public BookDimension clone(Timestamp validTo) {
            BookDimension newRecord = new BookDimension();
            newRecord.bookId = this.bookId;
            newRecord.authorId = this.authorId;
            newRecord.title = this.title;
            newRecord.isbn = this.isbn;
            newRecord.publishedDate = this.publishedDate;
            newRecord.genre = this.genre;
            newRecord.authorFirstName = this.authorFirstName;
            newRecord.authorLastName = this.authorLastName;
            newRecord.authorCountry = this.authorCountry;
            newRecord.bookSk = this.bookSk;
            newRecord.validFrom = this.validFrom;
            newRecord.validTo = validTo;
            return newRecord;
        }
    }

    public static class BooksSCD2ProcessFunction extends SCD2ProcessFunction<BookAuthor, BookDimension> {
        public BooksSCD2ProcessFunction() {
            super(TypeInformation.of(BookAuthor.class), TypeInformation.of(BookDimension.class),
                    BookDimension::new);
        }
    }

    public static class BookJsonParser implements MapFunction<String, Book> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        private double base64ToScaledDouble(String base64String, int scale) {
            byte[] decoded = Base64.getDecoder().decode(base64String);
            if (decoded.length < 8) {
                return 0.0;
            }
            double value = ByteBuffer.wrap(decoded).getDouble();
            BigDecimal bd = BigDecimal.valueOf(value)
                    .setScale(scale, RoundingMode.HALF_UP);
            return bd.doubleValue();
        }

        private Date convertDaysToSqlDate(int daysSinceEpoch) {
            long milliseconds = (long) daysSinceEpoch * 24 * 60 * 60 * 1000;
            return new Date(milliseconds);
        }

        @Override
        public Book map(String jsonString) throws Exception {
            JsonNode node = objectMapper.readTree(jsonString);
            Book book = new Book();

            book.bookId = node.get("book_id").asInt();
            book.title = node.get("title").asText();
            book.authorId = node.get("author_id").asInt();
            book.isbn = node.get("isbn").asText();
            book.price = base64ToScaledDouble(node.get("price").asText(), 2);
            book.publishedDate = convertDaysToSqlDate(node.get("published_date").asInt());
            book.description = node.get("description").asText();
            book.genre = node.get("genre").asText();
            book.stock = node.get("stock").asInt();
            book.op = node.get("op").asText();
            book.emittedTsMs = node.get("emitted_ts_ms").asLong();
            book.tsMs = node.get("ts_ms").asLong();
            book.connectorVersion = node.get("connector_version").asText();
            book.transactionId = node.get("transaction_id").asText();
            book.lsn = node.get("lsn").asLong();

            return book;
        }
    }

    public static class AuthorJsonParser implements MapFunction<String, Author> {
        private final ObjectMapper objectMapper = new ObjectMapper();

        @Override
        public Author map(String jsonString) throws Exception {
            JsonNode node = objectMapper.readTree(jsonString);
            Author author = new Author();

            author.authorId = node.get("author_id").asInt();
            author.firstName = node.get("first_name").asText();
            author.lastName = node.get("last_name").asText();
            author.biography = node.get("biography").asText();
            author.country = node.get("country").asText();

            author.op = node.get("op").asText();
            author.emittedTsMs = node.get("emitted_ts_ms").asLong();
            author.tsMs = node.get("ts_ms").asLong();
            author.connectorVersion = node.get("connector_version").asText();
            author.transactionId = node.get("transaction_id").asText();
            author.lsn = node.get("lsn").asLong();

            return author;
        }
    }

    public static class BookAuthorJoinFunction extends KeyedCoProcessFunction<Integer, Book, Author, BookAuthor> {
        // State to store the latest book and author records for each authorId
        private transient MapState<Integer, Book> latestBookState;
        private transient ValueState<Author> latestAuthorState;

        @Override
        public void open(OpenContext ctx) throws Exception {
            latestBookState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>("latestBookState", TypeInformation.of(Integer.class),
                            TypeInformation.of(Book.class)));
            latestAuthorState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("latestAuthor", TypeInformation.of(Author.class)));
        }

        @Override
        public void processElement1(Book book, Context context, Collector<BookAuthor> out)
                throws Exception {
            // A book record arrived. Store it and try to join with the latest author.
            latestBookState.put(book.bookId, book);

            Author currentAuthor = latestAuthorState.value();
            if (currentAuthor != null) {
                out.collect(createJoinedDimension(book, currentAuthor));
            }
            // If author is not yet available, we just store the book and wait.
            // You might set a timer here to handle cases where author never arrives.
        }

        @Override
        public void processElement2(Author author, Context context, Collector<BookAuthor> out)
                throws Exception {
            // An author record arrived. Store it.
            latestAuthorState.update(author);

            Iterable<Map.Entry<Integer, Book>> books = latestBookState.entries();
            if (books != null) {
                for (Map.Entry<Integer, Book> entry : books) {
                    Book currentBook = entry.getValue();
                    out.collect(createJoinedDimension(currentBook, author));
                }
            }
        }

        private BookAuthor createJoinedDimension(Book book, Author author) {
            BookAuthor dim = new BookAuthor();
            dim.bookId = book.bookId;
            dim.authorId = author.authorId;
            dim.title = book.title;
            dim.isbn = book.isbn;
            dim.publishedDate = book.publishedDate;
            dim.genre = book.genre;
            dim.firstName = author.firstName;
            dim.lastName = author.lastName;
            dim.country = author.country;
            // Debezium fields from book best for scd2 management
            dim.op = book.op;
            dim.tsMs = book.tsMs;
            dim.emittedTsMs = book.emittedTsMs;
            dim.connectorVersion = book.connectorVersion;
            dim.transactionId = book.transactionId;
            dim.lsn = book.lsn;
            return dim;
        }
    }

    public static void main(String[] args) throws Exception {
        String bookSourceTopic = BooksLandingJob.sinkTopic;
        String authorSourceTopic = AuthorsLandingJob.sinkTopic;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Books Streams
        KafkaSource<String> bookSource = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaProperties.bootStrapServers)
                .setTopics(bookSourceTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> bookRawStream = env.fromSource(bookSource,
                WatermarkStrategy.noWatermarks(),
                "Books Source");

        DataStream<Book> bookStream = bookRawStream
                .map(new BookJsonParser())
                .name("Parse JSON to Book")
                .keyBy(book -> book.authorId);

        // Author Stream
        KafkaSource<String> authorSource = KafkaSource.<String>builder()
                .setBootstrapServers(KafkaProperties.bootStrapServers)
                .setTopics(authorSourceTopic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> authorRawStream = env.fromSource(authorSource,
                WatermarkStrategy.noWatermarks(),
                "Author Source");

        DataStream<Author> authorStream = authorRawStream
                .map(new AuthorJsonParser())
                .name("Parse JSON to Book")
                .keyBy(author -> author.authorId);

        // Join Stream
        DataStream<BookAuthor> bookAuthorStream = bookStream.connect(authorStream)
                .process(new BookAuthorJoinFunction());

        // bookAuthorStream.print();

        DataStream<BookDimension> scd2Stream = bookAuthorStream
                .keyBy(record -> record.bookId)
                .process(new BooksSCD2ProcessFunction())
                .name("SCD2 Transformation");


        KafkaSink<BookDimension> sink = KafkaSink.<BookDimension>builder()
            .setBootstrapServers(KafkaProperties.bootStrapServers)
            .setRecordSerializer(
                KafkaRecordSerializationSchema.builder()
                .setTopic(sinkTopic)
                .setValueSerializationSchema(new PojoSerializer<BookDimension>())
                .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();

        // JdbcStatementBuilder<BookDimension> sinkStatement = (statement, book) -> {
        //     statement.setString(1, book.bookSk);
        //     statement.setInt(2, book.bookId);
        //     statement.setInt(3, book.authorId);
        //     statement.setString(4, book.title);
        //     statement.setString(5, book.isbn);
        //     statement.setDate(6, book.publishedDate);
        //     statement.setString(7, book.genre);
        //     statement.setString(8, book.authorFirstName);
        //     statement.setString(9, book.authorLastName);
        //     statement.setString(10, book.authorCountry);
        //     statement.setTimestamp(11, book.validFrom);
        //     statement.setTimestamp(12, book.validTo);
        // };

        // JdbcSink<BookDimension> sink = JdbcSink.<BookDimension>builder()
        //         .withExecutionOptions(JdbcExecutionOptions.builder()
        //                 .withBatchSize(1000)
        //                 .withBatchIntervalMs(200)
        //                 .withMaxRetries(5)
        //                 .build())
        //         .withQueryStatement(
        //                 """
        //                 INSERT INTO modeling_db.d_books (
        //                     bookSk, bookId, authorId, title, isbn, publishedDate, genre,
        //                     authorFirstName, authorLastName, authorCountry, validFrom, validTo
        //                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        //                     ON CONFLICT (bookSk) DO UPDATE SET
        //                         validTo = EXCLUDED.validTo 
        //                 """,
        //                 sinkStatement)
        //         .buildAtLeastOnce(DWConnectionCommonOptions.commonOptions);

        scd2Stream.sinkTo(sink);

        env.execute("d_books job");

    }
}
