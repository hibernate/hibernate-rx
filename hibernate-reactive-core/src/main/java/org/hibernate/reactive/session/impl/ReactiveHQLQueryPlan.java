/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import org.hibernate.Filter;
import org.hibernate.HibernateException;
import org.hibernate.action.internal.BulkOperationCleanupAction;
import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.query.spi.HQLQueryPlan;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.hql.spi.QueryTranslator;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.reactive.session.ReactiveQueryExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A reactific {@link HQLQueryPlan}
 */
class ReactiveHQLQueryPlan extends HQLQueryPlan {

	private static final CoreMessageLogger log = CoreLogging.messageLogger( HQLQueryPlan.class );

	public ReactiveHQLQueryPlan(
			String hql,
			boolean shallow,
			Map<String, Filter> enabledFilters,
			SessionFactoryImplementor factory) {
		super( hql, shallow, enabledFilters, factory );
	}

	public ReactiveHQLQueryPlan(
			String hql,
			boolean shallow,
			Map<String, Filter> enabledFilters,
			SessionFactoryImplementor factory, EntityGraphQueryHint entityGraphQueryHint) {
		super( hql, shallow, enabledFilters, factory, entityGraphQueryHint );
	}

	public ReactiveHQLQueryPlan(
			String hql,
			String collectionRole,
			boolean shallow,
			Map<String, Filter> enabledFilters,
			SessionFactoryImplementor factory,
			EntityGraphQueryHint entityGraphQueryHint) {
		super( hql, collectionRole, shallow, enabledFilters, factory, entityGraphQueryHint );
	}

	/**
	 * @deprecated Use performReactiveList instead
	 */
	@Deprecated
	@Override
	public List<Object> performList(
			QueryParameters queryParameters, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException( "Use performReactiveList instead" );
	}

	@Override
	public int performExecuteUpdate(QueryParameters queryParameters, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException( "Use performExecuteReactiveUpdate instead" );
	}

	/**
	 * @see HQLQueryPlan#performList(QueryParameters, SharedSessionContractImplementor)
	 */
	public CompletionStage<List<Object>> performReactiveList(QueryParameters queryParameters,
															 SharedSessionContractImplementor session)
			throws HibernateException {
		if ( log.isTraceEnabled() ) {
			log.tracev( "Find: {0}", getSourceQuery() );
			queryParameters.traceParameters( session.getFactory() );
		}

		// NOTE: In the superclass this is a private field.
		// getTranslators() creates a copy of the field array each time.
		final QueryTranslator[] translators = getTranslators();

		final RowSelection rowSelection = queryParameters.getRowSelection();
		final boolean hasLimit = rowSelection != null
				&& rowSelection.definesLimits();
		final boolean needsLimit = hasLimit && translators.length > 1;

		final QueryParameters queryParametersToUse;
		if ( needsLimit ) {
			log.needsLimit();
			final RowSelection selection = new RowSelection();
			selection.setFetchSize( queryParameters.getRowSelection().getFetchSize() );
			selection.setTimeout( queryParameters.getRowSelection().getTimeout() );
			queryParametersToUse = queryParameters.createCopyUsing( selection );
		}
		else {
			queryParametersToUse = queryParameters;
		}

		//fast path to avoid unnecessary allocation and copying
		if ( translators.length == 1 ) {
			ReactiveQueryTranslatorImpl reactiveTranslator = (ReactiveQueryTranslatorImpl) translators[0];
			return reactiveTranslator.reactiveList( session, queryParametersToUse );
		}
		final int guessedResultSize = guessResultSize( rowSelection );
		final List<Object> combinedResults = new ArrayList<>( guessedResultSize );
		final IdentitySet distinction;
		if ( needsLimit ) {
			distinction = new IdentitySet( guessedResultSize );
		}
		else {
			distinction = null;
		}
		AtomicInteger includedCount = new AtomicInteger( -1 );
		return CompletionStages.loop(
				translators,
				translator -> ((ReactiveQueryTranslatorImpl) translator)
						.reactiveList( session, queryParametersToUse )
						.thenAccept( tmpList -> {
							if ( needsLimit ) {
								needsLimitLoop( queryParameters, combinedResults, distinction, includedCount, tmpList );
							}
							else {
								combinedResults.addAll( tmpList );
							}
						} )
		).thenApply( v -> combinedResults );
	}

	private void needsLimitLoop(QueryParameters queryParameters,
								List<Object> combinedResults,
								IdentitySet distinction,
								AtomicInteger includedCount,
								List<Object> tmpList) {
		// NOTE : firstRow is zero-based
		RowSelection rowSelection = queryParameters.getRowSelection();
		final int first = rowSelection.getFirstRow() == null ? 0 : rowSelection.getFirstRow();
		final int max = rowSelection.getMaxRows() == null ? -1 : rowSelection.getMaxRows();
		for ( final Object result : tmpList ) {
			if ( !distinction.add( result ) ) {
				continue;
			}
			int included = includedCount.addAndGet( 1 );
			if ( included < first ) {
				continue;
			}
			combinedResults.add( result );
			if ( max >= 0 && included > max ) {
				return;
			}
		}
	}

	public CompletionStage<Integer> performExecuteReactiveUpdate(QueryParameters queryParameters,
																 ReactiveQueryExecutor session) {
		if ( log.isTraceEnabled() ) {
			log.tracev( "Execute update: {0}", getSourceQuery() );
			queryParameters.traceParameters( session.getFactory() );
		}
		QueryTranslator[] translators = getTranslators();
		if ( translators.length != 1 ) {
			log.splitQueries( getSourceQuery(), translators.length );
		}

		CompletionStage<Integer> combinedStage = CompletionStages.zeroFuture();
		for ( QueryTranslator translator : translators ) {
			ReactiveQueryTranslatorImpl reactiveTranslator = (ReactiveQueryTranslatorImpl) translator;

			session.addBulkCleanupAction( new BulkOperationCleanupAction(
					session.getSharedContract(),
					reactiveTranslator.getQuerySpaces()
			) );

			combinedStage = combinedStage
					.thenCompose(
							count -> reactiveTranslator.executeReactiveUpdate( queryParameters, session )
									.thenApply( updateCount -> count + updateCount )
					);
		}
		return combinedStage;
	}
}
