/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.loader;

import org.hibernate.HibernateException;
import org.hibernate.QueryException;
import org.hibernate.cache.spi.FilterKey;
import org.hibernate.cache.spi.QueryKey;
import org.hibernate.cache.spi.QueryResultsCache;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.loader.Loader;
import org.hibernate.reactive.adaptor.impl.PreparedStatementAdaptor;
import org.hibernate.reactive.event.impl.UnexpectedAccessToTheDatabase;
import org.hibernate.stat.spi.StatisticsImplementor;
import org.hibernate.transform.CacheableResultTransformer;
import org.hibernate.transform.ResultTransformer;
import org.hibernate.type.Type;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

import static org.hibernate.reactive.util.impl.CompletionStages.completedFuture;
import static org.hibernate.reactive.util.impl.CompletionStages.logSqlException;
import static org.hibernate.reactive.util.impl.CompletionStages.returnOrRethrow;

/**
 * Defines common reactive operations inherited by query loaders, in
 * particular, interaction with the cache.
 *
 * @see org.hibernate.loader.Loader
 *
 * @author Gavin King
 */
public interface CachingReactiveLoader<T> extends ReactiveLoader {

	CoreMessageLogger log = CoreLogging.messageLogger( Loader.class );

	default CompletionStage<List<Object>> doReactiveList(
			final String sql,
			final String queryIdentifier,
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final ResultTransformer forcedResultTransformer)
			throws HibernateException {

		final StatisticsImplementor statistics = session.getFactory().getStatistics();
		final boolean stats = statistics.isStatisticsEnabled();
		final long startTime = stats ? System.nanoTime() : 0;

		return doReactiveQueryAndInitializeNonLazyCollections( sql, session, queryParameters, true, forcedResultTransformer )
				.handle( (list, err) -> {
					logSqlException( err, () -> "could not execute query", sql );

					if ( err ==null && stats ) {
						final long endTime = System.nanoTime();
						final long milliseconds = TimeUnit.MILLISECONDS.convert( endTime - startTime, TimeUnit.NANOSECONDS );
						statistics.queryExecuted( queryIdentifier, list.size(), milliseconds );
					}

					return returnOrRethrow(err, list );
				} );
	}

	default CompletionStage<List<T>> reactiveListIgnoreQueryCache(
			String sql, String queryIdentifier,
			SharedSessionContractImplementor session,
			QueryParameters queryParameters) {
		return doReactiveList( sql, queryIdentifier, session, queryParameters, null )
				.thenApply( result -> getResultList( result, queryParameters.getResultTransformer() ) );
	}

	default CompletionStage<List<T>> reactiveListUsingQueryCache(
			final String sql,
			final String queryIdentifier,
			final SharedSessionContractImplementor session,
			final QueryParameters queryParameters,
			final Set<Serializable> querySpaces,
			final Type[] resultTypes) {

		QueryResultsCache queryCache = session.getFactory().getCache()
				.getQueryResultsCache( queryParameters.getCacheRegion() );

		QueryKey key = queryKey( sql, session, queryParameters );

		final List<Object> cachedList;
		try {
			cachedList = getReactiveResultFromQueryCache( session, queryParameters, querySpaces, resultTypes, queryCache, key );
		}
		catch (UnexpectedAccessToTheDatabase e) {
			log.debugf( "Some of the entities are not in the cache. The cache will be ignored for query: %s ", sql );

			// Some of the entities in the query results aren't cached and therefore it trys to load them from the db.
			// Currently this scenario causes an AssertionFailure exception because we cannot deal with the
			// CompletionStage in that phase.
			return reactiveListIgnoreQueryCache( sql, queryIdentifier, session, queryParameters );
		}

		CompletionStage<List<Object>> list;
		if ( cachedList == null ) {
			list = doReactiveList( sql, queryIdentifier, session, queryParameters, key.getResultTransformer() )
					.thenApply( cachableList -> {
						putReactiveResultInQueryCache( session, queryParameters, resultTypes, queryCache, key, cachableList );
						return cachableList;
					} );
		}
		else {
			list = completedFuture( cachedList );
		}

		return list.thenApply(
				result -> getResultList(
						transform( queryParameters, key, result,
								resolveResultTransformer( queryParameters.getResultTransformer() ) ),
						queryParameters.getResultTransformer()
				)
		);
	}

	default List<?> transform(QueryParameters queryParameters, QueryKey key, List<Object> result,
							  ResultTransformer resolvedTransformer) {
		if (resolvedTransformer == null) {
			return result;
		}
		else {
			CacheableResultTransformer transformer = key.getResultTransformer();
			if ( areResultSetRowsTransformedImmediately() ) {
				return transformer.retransformResults(
						result,
						getResultRowAliases(),
						queryParameters.getResultTransformer(),
						includeInResultRow()
				);
			}
			else {
				return transformer.untransformToTuples(result);
			}
		}
	}

	default QueryKey queryKey(String sql, SharedSessionContractImplementor session, QueryParameters queryParameters) {
		return QueryKey.generateQueryKey(
				sql,
				queryParameters,
				FilterKey.createFilterKeys( session.getLoadQueryInfluencers().getEnabledFilters() ),
				session,
				cacheableResultTransformer( queryParameters )
		);
	}

	default CacheableResultTransformer cacheableResultTransformer(QueryParameters queryParameters) {
		return CacheableResultTransformer.create(
				queryParameters.getResultTransformer(),
				getResultRowAliases(),
				includeInResultRow()
		);
	}

	boolean[] includeInResultRow();

	List<Object> getReactiveResultFromQueryCache(SharedSessionContractImplementor session, QueryParameters queryParameters, Set<Serializable> querySpaces, Type[] resultTypes, QueryResultsCache queryCache, QueryKey key);

	void putReactiveResultInQueryCache(SharedSessionContractImplementor session, QueryParameters queryParameters, Type[] resultTypes, QueryResultsCache queryCache, QueryKey key, List<Object> cachableList);

	ResultTransformer resolveResultTransformer(ResultTransformer resultTransformer);

	String[] getResultRowAliases();

	boolean areResultSetRowsTransformedImmediately();

	List<T> getResultList(List<?> results, ResultTransformer resultTransformer) throws QueryException;

	@Override
	default Object[] toParameterArray(QueryParameters queryParameters, SharedSessionContractImplementor session, LimitHandler limitHandler) {
		return PreparedStatementAdaptor.bind( adaptor -> bindToPreparedStatement(
				adaptor,
				queryParameters,
				limitHandler,
				session
		) );
	}

	void bindToPreparedStatement(PreparedStatement adaptor,
								 QueryParameters queryParameters,
								 LimitHandler limitHandler,
								 SharedSessionContractImplementor session) throws SQLException;
}
