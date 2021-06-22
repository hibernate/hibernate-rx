/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool;

import org.hibernate.Incubating;

import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Abstracts over reactive database connections, defining
 * operations that allow queries to be executed asynchronously
 * via {@link CompletionStage}.
 * <p>
 * It is illegal to perform two non-blocking operations concurrently
 * with a single {@code ReactiveConnection}. Instead, the second
 * operation must be chained on completion of the first operation.
 * This restriction might be relaxed in future, and is due to the
 * implementation of the {@code ProxyConnection} returned by
 * {@link org.hibernate.reactive.pool.impl.DefaultSqlClientPool#getProxyConnection()}.
 *
 * @see ReactiveConnectionPool
 */
@Incubating
public interface ReactiveConnection {

	@FunctionalInterface
	interface Expectation {
		void verifyOutcome(int rowCount, int batchPosition, String sql);
	}

	CompletionStage<Void> execute(String sql);
	CompletionStage<Void> executeOutsideTransaction(String sql);

	/**
	 * Run sql as statement (instead of preparedStatement)
	 */
	CompletionStage<Void> executeUnprepared(String sql);

	CompletionStage<Integer> update(String sql);
	CompletionStage<Integer> update(String sql, Object[] paramValues);
	CompletionStage<Void> update(String sql, Object[] paramValues, boolean allowBatching, Expectation expectation);
	CompletionStage<int[]> update(String sql, List<Object[]> paramValues);

	CompletionStage<Result> select(String sql);
	CompletionStage<Result> select(String sql, Object[] paramValues);
	CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues);

	CompletionStage<Long> insertAndSelectIdentifier(String sql, Object[] paramValues);
	CompletionStage<Long> selectIdentifier(String sql, Object[] paramValues);

	interface Result extends Iterator<Object[]> {
		int size();
	}

	CompletionStage<Void> beginTransaction();
	CompletionStage<Void> commitTransaction();
	CompletionStage<Void> rollbackTransaction();

	CompletionStage<Void> executeBatch();

	CompletionStage<Void> close();
}
