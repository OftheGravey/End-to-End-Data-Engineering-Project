package com.extractor.flink.join_functions;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.flink.streaming.api.operators.co.KeyedCoProcessOperator;

import com.extractor.flink.model.joined.BookAuthorJoined;
import com.extractor.flink.model.source.Author;
import com.extractor.flink.model.source.Book;

public class BookAuthorJoinFunctionTest {
        private KeyedTwoInputStreamOperatorTestHarness<Integer, Book, Author, BookAuthorJoined> testHarness;

        @BeforeEach
        public void setup() throws Exception {
                BookAuthorJoinFunction joinFunction = new BookAuthorJoinFunction();
                KeyedCoProcessOperator<Integer, Book, Author, BookAuthorJoined> operator = new KeyedCoProcessOperator<>(
                                joinFunction);

                testHarness = new KeyedTwoInputStreamOperatorTestHarness<>(
                                operator,
                                (Book b) -> b.bookId, // Key selector for books
                                (Author a) -> a.authorId, // Key selector for authors
                                TypeInformation.of(Integer.class));

                testHarness.open();
        }

        @AfterEach
        public void cleanup() throws Exception {
                testHarness.close();
        }

        @Test
        public void testBookArrivesFirst_thenAuthor() throws Exception {
                Book book = new Book();
                book.bookId = 1;
                book.authorId = 10;
                book.title = "Title A";
                book.isbn = "ISBN-A";

                Author author = new Author();
                author.authorId = 10;
                author.firstName = "John";
                author.lastName = "Doe";
                author.country = "US";

                testHarness.processElement1(book, 0);
                assertTrue(testHarness.extractOutputValues().isEmpty());

                testHarness.processElement2(author, 1);
                List<BookAuthorJoined> results = testHarness.extractOutputValues();
                assertEquals(1, results.size());

                BookAuthorJoined joined = results.get(0);
                assertEquals(1, joined.book.bookId);
                assertEquals("John", joined.author.firstName);
        }

        @Test
        public void testAuthorArrivesFirst_thenBook() throws Exception {
                Author author = new Author();
                author.authorId = 20;
                author.firstName = "Jane";
                author.lastName = "Smith";
                author.country = "UK";

                Book book = new Book();
                book.bookId = 2;
                book.authorId = 20;
                book.title = "Title B";
                book.isbn = "ISBN-B";

                testHarness.processElement2(author, 0);
                assertTrue(testHarness.extractOutputValues().isEmpty());

                testHarness.processElement1(book, 1);
                List<BookAuthorJoined> results = testHarness.extractOutputValues();
                assertEquals(1, results.size());

                BookAuthorJoined joined = results.get(0);
                assertEquals("Title B", joined.book.title);
                assertEquals("Smith", joined.author.lastName);
        }

        @Test
        public void testMultipleBooksForSameAuthor() throws Exception {
                Author author = new Author();
                author.authorId = 30;
                author.firstName = "Alice";
                author.lastName = "Walker";
                author.country = "CA";

                Book book1 = new Book();
                book1.bookId = 3;
                book1.authorId = 30;
                book1.title = "Book One";
                book1.isbn = "ISBN-1";

                Book book2 = new Book();
                book2.bookId = 4;
                book2.authorId = 30;
                book2.title = "Book Two";
                book2.isbn = "ISBN-2";

                testHarness.processElement1(book1, 0);
                testHarness.processElement1(book2, 1);
                assertTrue(testHarness.extractOutputValues().isEmpty());

                testHarness.processElement2(author, 2);
                List<BookAuthorJoined> results = testHarness.extractOutputValues();
                assertEquals(2, results.size());

                assertEquals("Book One", results.get(0).book.title);
                assertEquals("Book Two", results.get(1).book.title);
                assertEquals("Alice", results.get(0).author.firstName);
        }
}
