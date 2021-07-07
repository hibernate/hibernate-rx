/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.util.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;

import com.ibm.asyncutil.iteration.AsyncIterator;

import static com.ibm.asyncutil.iteration.AsyncTrampoline.asyncWhile;

public class CompletionStages {

	private static final CoreMessageLogger log =
			CoreLogging.messageLogger("org.hibernate.reactive.errors");

	// singleton instances:
	private static final CompletionStage<Void> VOID = completedFuture( null );
	private static final CompletionStage<Integer> ZERO = completedFuture( 0 );
	private static final CompletionStage<Boolean> TRUE = completedFuture( true );
	private static final CompletionStage<Boolean> FALSE = completedFuture( false );

	private static <T> boolean alwaysTrue(T o, int index) {
		return true;
	}

	private static boolean alwaysTrue(int index) {
		return true;
	}

	private static boolean alwaysTrue(Object o) {
		return true;
	}

	private static CompletionStage<Boolean> alwaysContinue(Object ignored) {
		return TRUE;
	}

	public static CompletionStage<Void> voidFuture(Object ignore) {
		return voidFuture();
	}

	public static CompletionStage<Void> voidFuture() {
		return VOID;
	}

	public static CompletionStage<Integer> zeroFuture() {
		return ZERO;
	}

	public static CompletionStage<Integer> zeroFuture(Object ignore) {
		return zeroFuture();
	}

	public static CompletionStage<Boolean> trueFuture() {
		return TRUE;
	}
	public static CompletionStage<Boolean> falseFuture() {
		return FALSE;
	}

	public static <T> CompletionStage<T> nullFuture() {
		//Unsafe cast, but perfectly fine: avoids having to allocate a new instance
		//for each different "type of null".
		return (CompletionStage<T>) VOID;
	}

	public static <T> CompletionStage<T> completedFuture(T value) {
		return CompletableFuture.completedFuture( value );
	}

	public static <T> CompletionStage<T> failedFuture(Throwable t) {
		CompletableFuture<T> ret = new CompletableFuture<>();
		ret.completeExceptionally( t );
		return ret;
	}

	public static <T extends Throwable, Ret> Ret rethrow(Throwable x) throws T {
		throw (T) x;
	}

	public static <T extends Throwable, Ret> Ret returnNullorRethrow(Throwable x) throws T {
		if ( x != null ) {
			throw (T) x;
		}
		return null;
	}

	public static <T extends Throwable, Ret> Ret returnOrRethrow(Throwable x, Ret result) throws T {
		if ( x != null ) {
			throw (T) x;
		}
		return result;
	}

	/**
	 * For CompletionStage#handle when we don't care about errors
	 */
	public static <U> U ignoreErrors(Void unused, Throwable throwable) {
		return null;
	}

	public static void logSqlException(Throwable t, Supplier<String> message, String sql) {
		if ( t != null ) {
			log.error( "failed to execute statement [" + sql + "]" );
			log.error( message.get(), t );
		}
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int total = 0;
	 * for ( int i = start; i < end; i++ ) {
	 *   total = total + consumer.apply( i );
	 * }
	 * </pre>
	 */
	public static CompletionStage<Integer> total(int start, int end, IntFunction<CompletionStage<Integer>> consumer) {
		return AsyncIterator.range( start, end )
				.thenCompose( i -> consumer.apply( i.intValue() ) )
				.fold( 0, Integer::sum );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int total = 0;
	 * while( iterator.hasNext() ) {
	 *   total += consumer.apply( iterator.next() );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Integer> total(Iterator<T> iterator, Function<T,CompletionStage<Integer>> consumer) {
		return AsyncIterator.fromIterator( iterator )
				.thenCompose( consumer )
				.fold( 0, Integer::sum );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int total = 0;
	 * for ( int i = start; i < end; i++ ) {
	 *   total = total + consumer.apply( array[i] );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Integer> total(T[] array, Function<T, CompletionStage<Integer>> consumer) {
		return total( 0, array.length, index -> consumer.apply( array[index] ) );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   consumer.apply( array[i] );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(T[] array, Function<T, CompletionStage<?>> consumer) {
		return loop( 0, array.length, index -> consumer.apply( array[index] ) );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   if ( filter.test(i) )  {
	 *   	consumer.apply( i );
	 *   }
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(T[] array, IntPredicate filter, IntFunction<CompletionStage<?>> consumer) {
		return loop( 0, array.length, filter, consumer );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * int index = 0
	 * while( iterator.hasNext() ) {
	 *   consumer.apply( iterator.next(), index++ );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Iterator<T> iterator, IntBiFunction<T, CompletionStage<?>> consumer) {
		return loop( iterator, CompletionStages::alwaysTrue, consumer );
	}
	/**
	 * Equivalent to:
	 * <pre>
	 * int index = -1
	 * while( iterator.hasNext() ) {
	 *   index++
	 *   T next = iterator.next();
	 *   if (filter.test( next, index ) {
	 *     consumer.apply( next, index );
	 *   }
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Iterator<T> iterator, IntBiPredicate<T> filter, IntBiFunction<T, CompletionStage<?>> consumer) {
		if ( iterator.hasNext() ) {
			final IndexedIteratorLoop<T> loop = new IndexedIteratorLoop<>( iterator, filter, consumer );
			return asyncWhile( loop::next );
		}
		return voidFuture();
	}

	/**
	 * It represents a loop on an iterator that requires
	 * an index of the current element.
	 * <p>
	 * Equivalent to:
	 * <pre>
	 *   int index = -1
	 * 	 while( iterator.hasNext() ) {
	 * 	   index++
	 * 	   T next = iterator.next();
	 * 	   if (filter.test( next, index ) {
	 * 	     consumer.apply( next, index );
	 * 	   }
	 * 	 }
	 * </pre>
	 * </p>
	 * <p>
	 * This class keeps track of the state of the loop, allowing us to
	 * use an {@code AsyncTrampoline#asyncWhile} via method reference.
	 * </p>
	 * @see com.ibm.asyncutil.iteration.AsyncTrampoline
	 * @param <T> the class of the elements in the iterator
	 */
	private static class IndexedIteratorLoop<T> {
		private final IntBiPredicate<T> filter;
		private final IntBiFunction<T, CompletionStage<?>> consumer;
		private final Iterator<T> iterator;
		private int currentIndex = -1;
		private T currentEntry;

		public IndexedIteratorLoop(Iterator<T> iterator, IntBiPredicate<T> filter, IntBiFunction<T, CompletionStage<?>> consumer) {
			this.iterator = iterator;
			this.filter = filter;
			this.consumer = consumer;
		}

		public CompletionStage<Boolean> next() {
			if ( hasNext() ) {
				final T entry = currentEntry;
				final int index = currentIndex;
				return consumer.apply( entry, index )
						.thenCompose( CompletionStages::alwaysContinue );
			}
			return FALSE;
		}

		// Skip all the indexes not matching the filter
		private boolean hasNext() {
			int index = currentIndex;
			T next = currentEntry;
			boolean hasNext = false;
			while ( iterator.hasNext() ) {
				next = iterator.next();
				index++;
				if ( filter.test( next, index ) ) {
					hasNext = true;
					break;
				}
			}
			this.currentEntry = next;
			this.currentIndex = index;
			return hasNext;
		}
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( Object next : iterable ) {
	 *   consumer.apply( next );
	 * }
	 * </pre>
	 */
	public static <T> CompletionStage<Void> loop(Collection<T> collection, Function<T, CompletionStage<?>> consumer) {
		return loop( collection, CompletionStages::alwaysTrue, consumer );
	}

	/**
	 * @deprecated always prefer the variants which use a List rather than a Collection
	 */
	@Deprecated
	public static <T> CompletionStage<Void> loop(Collection<T> collection, Predicate<T> filter, Function<T, CompletionStage<?>> consumer) {
		if ( collection instanceof List ) {
			//This is true in almost all known cases, so a good optimisation in practice.
			return loop( (List<T>) collection, filter, consumer );
		}
		else {
			final List<T> list = new ArrayList<>( collection );
			return loop( list, filter, consumer );
		}
	}

	public static <T> CompletionStage<Void> loop(List<T> list, Function<T, CompletionStage<?>> consumer) {
		return loop( list, CompletionStages::alwaysTrue, consumer );
	}

	public static <T> CompletionStage<Void> loop(List<T> list, Predicate<T> filter, Function<T,CompletionStage<?>> consumer) {
		return loop( 0, list.size(), index -> filter.test( list.get( index ) ), index -> consumer.apply( list.get( index ) ) );
	}

	public static <T> CompletionStage<Void> loop(Queue<T> queue, Function<T, CompletionStage<?>> consumer) {
		return loop( queue.iterator(), CompletionStages::alwaysTrue , (value, integer) -> consumer.apply( value ) );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   consumer.apply( i );
	 * }
	 * </pre>
	 */
	public static CompletionStage<Void> loop(int start, int end, IntFunction<CompletionStage<?>> consumer) {
		return loop( start, end, CompletionStages::alwaysTrue, consumer );
	}

	/**
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   if ( filter.test(i) ) {
	 *   	consumer.apply( i );
	 *   }
	 * }
	 * </pre>
	 */
	public static CompletionStage<Void> loop(int start, int end, IntPredicate filter, IntFunction<CompletionStage<?>> consumer) {
		if ( start < end ) {
			final ArrayLoop loop = new ArrayLoop( start, end, filter, consumer);
			return asyncWhile( loop::next );
		}
		return voidFuture();
	}

	/**
	 * The status of a loop over an array.
	 * <p>
	 * Equivalent to:
	 * <pre>
	 * for ( int i = start; i < end; i++ ) {
	 *   if ( filter.test( i ) ) {
	 *   	consumer.apply( i );
	 *   }
	 * }
	 * </pre>
	 * </p>
	 * <p>
	 * This class keeps track of the state of the loop, allowing us to
	 * use an {@code AsyncTrampoline#asyncWhile} via method reference.
	 * </p>
	 */
	private static class ArrayLoop {

		private final IntPredicate filter;
		private final IntFunction<CompletionStage<?>> consumer;
		private final int end;
		private int current;

		public ArrayLoop(int start, int end, IntPredicate filter, IntFunction<CompletionStage<?>> consumer) {
			this.end = end;
			this.filter = filter;
			this.consumer = consumer;
			this.current = start;
		}

		public CompletionStage<Boolean> next() {
			current = next( current );
			if ( current < end ) {
				final int index = current++;
				return consumer.apply( index )
						.thenCompose( CompletionStages::alwaysContinue );
			}
			return FALSE;
		}

		/**
		 * @param start the first index to test
		 * @return the next valid index
		 */
		private int next(int start) {
			int index = start;
			while ( index < end && !filter.test( index ) ) {
				index++;
			}
			return index;
		}
	}

	public static CompletionStage<Void> applyToAll(Function<Object, CompletionStage<?>> op, Object[] entity) {
		switch ( entity.length ) {
			case 0: return nullFuture();
			case 1: return op.apply( entity[0] ).thenCompose( CompletionStages::voidFuture );
			default: return CompletionStages.loop( entity, op );
		}
	}
}
