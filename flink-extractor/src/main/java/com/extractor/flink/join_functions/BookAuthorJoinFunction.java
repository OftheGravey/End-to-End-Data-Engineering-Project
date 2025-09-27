package com.extractor.flink.join_functions;

import java.util.Map;
import org.apache.flink.util.Collector;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;

import com.extractor.flink.model.joined.BookAuthorJoined;
import com.extractor.flink.model.source.Author;
import com.extractor.flink.model.source.Book;

// Left join Authors and their books
// Authors and books are assumed to be added at the same time
// Unmatched records will be ignored and left to the TTL.
public class BookAuthorJoinFunction extends KeyedCoProcessFunction<Integer, Book, Author, BookAuthorJoined> {
	private transient MapState<Integer, Book> latestBookState;
	private transient ValueState<Author> latestAuthorState;

	@Override
	public void open(OpenContext ctx) throws Exception {
		latestBookState = getRuntimeContext().getMapState(new MapStateDescriptor<>("latestBookState",
				TypeInformation.of(Integer.class), TypeInformation.of(Book.class)));
		latestAuthorState = getRuntimeContext()
				.getState(new ValueStateDescriptor<>("latestAuthor", TypeInformation.of(Author.class)));
	}

	@Override
	public void processElement1(Book book, Context context, Collector<BookAuthorJoined> out) throws Exception {
		latestBookState.put(book.bookId, book);

		Author currentAuthor = latestAuthorState.value();
		if (currentAuthor != null) {
			out.collect(createJoinedDimension(book, currentAuthor));
		}
	}

	@Override
	public void processElement2(Author author, Context context, Collector<BookAuthorJoined> out) throws Exception {
		latestAuthorState.update(author);

		Iterable<Map.Entry<Integer, Book>> books = latestBookState.entries();
		if (books != null) {
			for (Map.Entry<Integer, Book> entry : books) {
				Book currentBook = entry.getValue();
				out.collect(createJoinedDimension(currentBook, author));
			}
		}
	}

	private BookAuthorJoined createJoinedDimension(Book book, Author author) {
		BookAuthorJoined dim = new BookAuthorJoined(book, author);
		return dim;
	}
}