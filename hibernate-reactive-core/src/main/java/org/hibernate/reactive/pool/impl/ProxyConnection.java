/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;

import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.util.impl.CompletionStages;

import static org.hibernate.reactive.common.InternalStateAssertions.assertUseOnEventLoop;

/**
 * A proxy {@link ReactiveConnection} that initializes the
 * underlying connection lazily.
 */
final class ProxyConnection implements ReactiveConnection {

	private final ReactiveConnectionPool sqlClientPool;
	private ReactiveConnection connection;
	private boolean connected;
	private boolean closed;
	private final String tenantId;

	public ProxyConnection(ReactiveConnectionPool sqlClientPool) {
		this.sqlClientPool = sqlClientPool;
		tenantId = null;
	}

	public ProxyConnection(ReactiveConnectionPool sqlClientPool, String tenantId) {
		this.sqlClientPool = sqlClientPool;
		this.tenantId = tenantId;
	}

	private <T> CompletionStage<T> withConnection(Function<ReactiveConnection, CompletionStage<T>> operation) {
		assertUseOnEventLoop();
		if ( closed ) {
			CompletableFuture<T> ret = new CompletableFuture<>();
			ret.completeExceptionally( new IllegalStateException( "session is closed" ) );
			return ret;
		}
		if ( !connected ) {
			connected = true; // we're not allowed to fetch two connections!
			CompletionStage<ReactiveConnection> connection =
					tenantId == null ? sqlClientPool.getConnection() : sqlClientPool.getConnection( tenantId );
			return connection.thenApply( newConnection -> this.connection = newConnection )
					.thenCompose( operation );
		}
		else {
			if ( connection == null ) {
				// we're already in the process of fetching a connection,
				// so this must be an illegal concurrent call
				CompletableFuture<T> ret = new CompletableFuture<>();
				ret.completeExceptionally( new IllegalStateException( "session is currently connecting to database" ) );
				return ret;
			}
			return operation.apply( connection );
		}
	}

	@Override
	public CompletionStage<Void> execute(String sql) {
		return withConnection( conn -> conn.execute( sql ) );
	}

	@Override
	public CompletionStage<Void> executeStatement(String sql) {
		return withConnection( conn -> conn.executeStatement( sql ) );
	}

	@Override
	public CompletionStage<Void> executeOutsideTransaction(String sql) {
		return withConnection( conn -> conn.executeOutsideTransaction( sql ) );
	}

	@Override
	public CompletionStage<Integer> update(String sql) {
		return withConnection( conn -> conn.update( sql ) );
	}

	@Override
	public CompletionStage<Integer> update(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.update( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Void> update(
			String sql,
			Object[] paramValues,
			boolean allowBatching,
			Expectation expectation) {
		return withConnection( conn -> conn.update( sql, paramValues, false, expectation ) );
	}

	@Override
	public CompletionStage<int[]> update(String sql, List<Object[]> paramValues) {
		return withConnection( conn -> conn.update( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Long> insertAndSelectIdentifier(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.insertAndSelectIdentifier( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Result> select(String sql) {
		return withConnection( conn -> conn.select( sql ) );
	}

	@Override
	public CompletionStage<Result> select(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.select( sql, paramValues ) );
	}

	@Override
	public CompletionStage<ResultSet> selectJdbc(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.selectJdbc( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Long> selectIdentifier(String sql, Object[] paramValues) {
		return withConnection( conn -> conn.selectIdentifier( sql, paramValues ) );
	}

	@Override
	public CompletionStage<Void> beginTransaction() {
		return withConnection( ReactiveConnection::beginTransaction );
	}

	@Override
	public CompletionStage<Void> commitTransaction() {
		return withConnection( ReactiveConnection::commitTransaction );
	}

	@Override
	public CompletionStage<Void> rollbackTransaction() {
		return withConnection( ReactiveConnection::rollbackTransaction );
	}

	@Override
	public CompletionStage<Void> executeBatch() {
		return withConnection( ReactiveConnection::executeBatch );
	}

	@Override
	public CompletionStage<Void> close() {
		CompletionStage<Void> stage = CompletionStages.voidFuture();
		if ( connection != null ) {
			stage = stage.thenCompose( v -> connection.close() );

		}
		return stage.thenAccept( v -> {
			connection = null;
			closed = true;
		} );
	}

}
