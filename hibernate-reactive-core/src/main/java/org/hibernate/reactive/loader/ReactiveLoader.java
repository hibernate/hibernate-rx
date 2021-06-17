/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader;

import org.hibernate.JDBCException;
import org.hibernate.dialect.PostgreSQL9Dialect;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.dialect.pagination.LimitHelper;
import org.hibernate.dialect.pagination.NoopLimitHandler;
import org.hibernate.engine.spi.*;
import org.hibernate.loader.spi.AfterLoadAction;
import org.hibernate.reactive.adaptor.impl.QueryParametersAdaptor;
import org.hibernate.reactive.engine.impl.ReactivePersistenceContextAdapter;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveConnectionSupplier;
import org.hibernate.transform.ResultTransformer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * Defines common reactive operations inherited by all kinds of loaders.
 *
 * @see org.hibernate.loader.Loader
 *
 * @author Gavin King
 */
public interface ReactiveLoader {

	default boolean isPostgresSQL(SharedSessionContractImplementor session) {
		return session.getJdbcServices().getDialect() instanceof PostgreSQL9Dialect;
	}

	default CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final String sql,
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters) {
		return doReactiveQueryAndInitializeNonLazyCollections(sql, session, queryParameters, false, null);
	}

	default CompletionStage<List<Object>> doReactiveQueryAndInitializeNonLazyCollections(
			final String sql,
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final boolean returnProxies,
			final ResultTransformer forcedResultTransformer) {
		final PersistenceContext persistenceContext = session.getPersistenceContext();
		boolean defaultReadOnlyOrig = persistenceContext.isDefaultReadOnly();
		if ( queryParameters.isReadOnlyInitialized() ) {
			// The read-only/modifiable mode for the query was explicitly set.
			// Temporarily set the default read-only/modifiable setting to the query's setting.
			persistenceContext.setDefaultReadOnly( queryParameters.isReadOnly() );
		}
		else {
			// The read-only/modifiable setting for the query was not initialized.
			// Use the default read-only/modifiable from the persistence context instead.
			queryParameters.setReadOnly( persistenceContext.isDefaultReadOnly() );
		}
		persistenceContext.beforeLoad();

		final List<AfterLoadAction> afterLoadActions = new ArrayList<>();

		return executeReactiveQueryStatement(
				sql,
				queryParameters,
				afterLoadActions,
				session
		)
				.thenCompose( resultSet -> {
							discoverTypes( queryParameters, resultSet );
							return reactiveProcessResultSet(
									resultSet,
									queryParameters,
									session,
									returnProxies,
									forcedResultTransformer,
									afterLoadActions
							);
				} )
				.whenComplete( (list, e) -> persistenceContext.afterLoad() )
				.thenCompose( list ->
						// only initialize non-lazy collections after everything else has been refreshed
						((ReactivePersistenceContextAdapter) persistenceContext ).reactiveInitializeNonLazyCollections()
								.thenApply(v -> list)
				)
				.whenComplete( (list, e) -> persistenceContext.setDefaultReadOnly(defaultReadOnlyOrig) );
	}

	Parameters parameters();

	default CompletionStage<ResultSet> executeReactiveQueryStatement(
			String sqlStatement,
			QueryParameters queryParameters,
			List<AfterLoadAction> afterLoadActions,
			SharedSessionContractImplementor session) {

		// Processing query filters.
		queryParameters.processFilters( sqlStatement, session );

		// Applying LIMIT clause.
		final LimitHandler limitHandler = limitHandler( queryParameters.getRowSelection(), session );
		String sql = limitHandler.processSql( queryParameters.getFilteredSQL(), queryParameters.getRowSelection() );

		// Adding locks and comments.
		sql = preprocessSQL( sql, queryParameters, session.getFactory(), afterLoadActions );
		Object[] parameterArray = toParameterArray( queryParameters, session, limitHandler );

		boolean hasFilter = session.getLoadQueryInfluencers().hasEnabledFilters();
		if ( hasFilter ) {
			// If the query is using filters, we know that they have been applied when we
			// reach this point. Therefore it is safe to process the query.
			sql = parameters().process( sql );
		}
		else if ( LimitHelper.useLimit( limitHandler, queryParameters.getRowSelection() ) ) {
			// if the query is not using filters, it should have been preprocessed before
			// but limit and offset are only applied before execution and therefore we
			// might have some additional parameters to process.
			sql = parameters().processLimit( sql, parameterArray, LimitHelper.hasFirstRow( queryParameters.getRowSelection() ) );
		}

		return ((ReactiveConnectionSupplier) session).getReactiveConnection()
				.selectJdbc( sql, parameterArray );
	}

	default LimitHandler limitHandler(RowSelection selection, SharedSessionContractImplementor session) {
		LimitHandler limitHandler = session.getJdbcServices().getDialect().getLimitHandler();
		return LimitHelper.useLimit( limitHandler, selection ) ? limitHandler : NoopLimitHandler.INSTANCE;
	}

	default CompletionStage<List<Object>> reactiveProcessResultSet(
			ResultSet rs,
			QueryParameters queryParameters,
			SharedSessionContractImplementor session,
			boolean returnProxies,
			ResultTransformer forcedResultTransformer,
			List<AfterLoadAction> afterLoadActions) {
		try {
			return getReactiveResultSetProcessor()
					.reactiveExtractResults(
							rs,
							session,
							queryParameters,
							null,
							returnProxies,
							queryParameters.isReadOnly( session ),
							forcedResultTransformer,
							afterLoadActions
					);
		}
		catch (SQLException sqle) {
			//don't log or convert it - just pass it on to the caller
			throw new JDBCException( "could not load batch", sqle );
		}
	}

	ReactiveResultSetProcessor getReactiveResultSetProcessor();

	/**
	 * Used by query loaders to add stuff like locking and hints/comments
	 *
	 * @see org.hibernate.loader.Loader#preprocessSQL(String, QueryParameters, SessionFactoryImplementor, List)
	 */
	default String preprocessSQL(String sql, QueryParameters queryParameters, SessionFactoryImplementor factory,
						 List<AfterLoadAction> afterLoadActions) {
		// I believe this method is only needed for query-type loaders
		return sql;
	}

	/**
	 * Used by {@link org.hibernate.reactive.loader.custom.impl.ReactiveCustomLoader}
	 * when there is no result set mapping.
	 */
	default void discoverTypes(QueryParameters queryParameters, ResultSet resultSet) {}

	default Object[] toParameterArray(QueryParameters queryParameters, SharedSessionContractImplementor session, LimitHandler limitHandler) {
		return QueryParametersAdaptor.arguments( queryParameters, session, limitHandler );
	}
}
