package com.extractor.flink.model.joined;

import com.extractor.flink.model.source.Author;
import com.extractor.flink.model.source.Book;
import com.extractor.flink.model.source.DebeziumSourceRecord;

public class BookAuthorJoined extends DebeziumSourceRecord {
    public Author author;
    public Book book;

    public BookAuthorJoined(Book book, Author author) {
        this.book = book;
        this.author = author;
        this.tsMs = book.tsMs;
        this.op = book.op;
    }
}