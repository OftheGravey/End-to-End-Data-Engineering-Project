package com.extractor.flink.model.dimensions;

import java.sql.Date;
import java.util.UUID;

import com.extractor.flink.model.joined.BookAuthorJoined;

public class BookDimension extends TargetDimensionRecord {
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

		public BookDimension(BookAuthorJoined record, Long validTo) {
			super(record.book, validTo);
			this.bookId = record.book.bookId;
			this.authorId = record.book.authorId;
			this.title = record.book.title;
			this.isbn = record.book.isbn;
			this.publishedDate = record.book.publishedDate;
			this.genre = record.book.genre;
			this.authorFirstName = record.author.firstName;
			this.authorLastName = record.author.lastName;
			this.authorCountry = record.author.country;
			this.bookSk = UUID.randomUUID().toString();
		}

		public BookDimension() {
		};

		@Override
		public BookDimension clone(Long validTo) {
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