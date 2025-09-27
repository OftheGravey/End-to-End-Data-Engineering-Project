package com.extractor.flink.model.source;

import java.sql.Date;

public class Book extends DebeziumSourceRecord {
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

	public Book() {
	}

	@Override
	public String toString() {
		return String.format("Book{bookId=%d, title='%s', op='%s'}", bookId, title, op);
	}
}
