/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.session.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.engine.query.spi.EntityGraphQueryHint;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.RowSelection;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.hql.internal.QueryExecutionRequestException;
import org.hibernate.hql.internal.ast.HqlSqlWalker;
import org.hibernate.hql.internal.ast.QuerySyntaxException;
import org.hibernate.hql.internal.ast.QueryTranslatorImpl;
import org.hibernate.hql.internal.ast.SqlGenerator;
import org.hibernate.hql.internal.ast.exec.MultiTableDeleteExecutor;
import org.hibernate.hql.internal.ast.exec.MultiTableUpdateExecutor;
import org.hibernate.hql.internal.ast.exec.StatementExecutor;
import org.hibernate.hql.internal.ast.tree.QueryNode;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.IdentitySet;
import org.hibernate.loader.hql.QueryLoader;
import org.hibernate.param.ParameterSpecification;
import org.hibernate.reactive.bulk.StatementsWithParameters;
import org.hibernate.reactive.loader.hql.impl.ReactiveQueryLoader;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveQueryExecutor;

import org.jboss.logging.Logger;

import antlr.RecognitionException;
import antlr.collections.AST;


/**
 * A {@link org.hibernate.hql.spi.FilterTranslator} that adds reactive versions of the
 * existing methods in the interface and throw an exception if the non-reactive variant
 * is invoked.
 *
 * throws {@link UnsupportedOperationException} if a non reactive method is invoked
 */
public class ReactiveQueryTranslatorImpl<T> extends QueryTranslatorImpl {

	private final Parameters parameters;

	private ReactiveQueryLoader<T> queryLoader;

	private static final CoreMessageLogger LOG = Logger.getMessageLogger(
			CoreMessageLogger.class,
			ReactiveQueryTranslatorImpl.class.getName()
	);

	private final SessionFactoryImplementor factory;

	public ReactiveQueryTranslatorImpl(
			String queryIdentifier,
			String query,
			Map enabledFilters,
			SessionFactoryImplementor factory) {
		super( queryIdentifier, query, enabledFilters, factory );
		this.factory = factory;
		this.parameters = Parameters.instance( factory.getJdbcServices().getDialect() );
	}

	public ReactiveQueryTranslatorImpl(
			String queryIdentifier,
			String query,
			Map enabledFilters,
			SessionFactoryImplementor factory, EntityGraphQueryHint entityGraphQueryHint) {
		super( queryIdentifier, query, enabledFilters, factory, entityGraphQueryHint );
		this.factory = factory;
		this.parameters = Parameters.instance( factory.getJdbcServices().getDialect() );
	}

	@Override
	protected QueryLoader createQueryLoader(HqlSqlWalker w, SessionFactoryImplementor factory) {
		queryLoader = new ReactiveQueryLoader<>( this, factory, w.getSelectClause() );
		return queryLoader;
	}

	/**
	 * @deprecated Use {@link #list(SharedSessionContractImplementor, QueryParameters)} instead.
	 */
	@Deprecated
	@Override
	public List<Object> list(SharedSessionContractImplementor session, QueryParameters queryParameters)
			throws HibernateException {
		throw new UnsupportedOperationException("Use #reactiveList instead");
	}

	/**
	 * The reactive version of {@link QueryTranslatorImpl#list(SharedSessionContractImplementor, QueryParameters)}.
	 */
	public CompletionStage<List<T>> reactiveList(SharedSessionContractImplementor session,
													  QueryParameters queryParameters)
			throws HibernateException {
		// Delegate to the QueryLoader...
		errorIfDML();

		final QueryNode query = (QueryNode) getSqlAST();
		final boolean hasLimit =
				queryParameters.getRowSelection() != null
						&& queryParameters.getRowSelection().definesLimits();
		final boolean needsDistincting =
				( query.getSelectClause().isDistinct() || getEntityGraphQueryHint() != null || hasLimit )
						&& containsCollectionFetches();

		QueryParameters queryParametersToUse;
		if ( hasLimit && containsCollectionFetches() ) {
			boolean fail = session.getFactory().getSessionFactoryOptions().isFailOnPaginationOverCollectionFetchEnabled();
			if (fail) {
				throw new HibernateException("firstResult/maxResults specified with collection fetch. " +
						"In memory pagination was about to be applied. " +
						"Failing because 'Fail on pagination over collection fetch' is enabled.");
			}
			else {
				LOG.firstOrMaxResultsSpecifiedWithCollectionFetch();
			}
			RowSelection selection = new RowSelection();
			selection.setFetchSize( queryParameters.getRowSelection().getFetchSize() );
			selection.setTimeout( queryParameters.getRowSelection().getTimeout() );
			queryParametersToUse = queryParameters.createCopyUsing( selection );
		}
		else {
			queryParametersToUse = queryParameters;
		}

		return queryLoader.reactiveList( session, queryParametersToUse )
				.thenApply( results -> {
					if ( needsDistincting ) {
						int includedCount = -1;
						// NOTE : firstRow is zero-based
						int first = !hasLimit || queryParameters.getRowSelection().getFirstRow() == null
								? 0
								: queryParameters.getRowSelection().getFirstRow();
						int max = !hasLimit || queryParameters.getRowSelection().getMaxRows() == null
								? -1
								: queryParameters.getRowSelection().getMaxRows();
						List<T> tmp = new ArrayList<>();
						IdentitySet distinction = new IdentitySet();
						for ( T result : results ) {
							if ( !distinction.add( result ) ) {
								continue;
							}
							includedCount++;
							if ( includedCount < first ) {
								continue;
							}
							tmp.add( result );
							// NOTE : ( max - 1 ) because first is zero-based while max is not...
							if ( max >= 0 && ( includedCount - first ) >= ( max - 1 ) ) {
								break;
							}
						}
						return tmp;
					}
					return results;
				} );
	}

	/**
	 * The reactive version of
	 * {@link QueryTranslatorImpl#executeUpdate(QueryParameters, SharedSessionContractImplementor)}.
	 */
	public CompletionStage<Integer> executeReactiveUpdate(QueryParameters queryParameters, ReactiveQueryExecutor session) {
		errorIfSelect();

		return getUpdateHandler().execute( session, queryParameters );
	}

	private String[] process(String[] sqlStatements, int paramLength) {
		String[] processed = new String[sqlStatements.length];
		for ( int i = 0; i < processed.length; i++ ) {
			processed[i] = parameters.process( sqlStatements[i], paramLength );
		}
		return processed;
	}

	private StatementsWithParameters getUpdateHandler() {
		StatementExecutor executor = getStatementExecutor();
		if (executor instanceof MultiTableUpdateExecutor) {
			return (StatementsWithParameters) ((MultiTableUpdateExecutor) executor).getUpdateHandler();
		}
		else if (executor instanceof MultiTableDeleteExecutor) {
			return (StatementsWithParameters) ((MultiTableDeleteExecutor) executor).getDeleteHandler();
		}
		else {
			return new StatementsWithParameters() {
				final ParameterSpecification[] parameterSpecifications = getCollectedParameterSpecifications().toArray( new ParameterSpecification[0] );
				final String[] statements = process( executor.getSqlStatements(), parameterSpecifications.length );

				@Override
				public String[] getSqlStatements() {
					return statements;
				}

				@Override
				public ParameterSpecification[][] getParameterSpecifications() {
					ParameterSpecification[][] result = new ParameterSpecification[statements.length][];
					Arrays.fill( result, parameterSpecifications );
					return result;
				}
			};
		}
	}


	/**
	 * @deprecated Use {@link #executeReactiveUpdate(QueryParameters, ReactiveQueryExecutor)}
	 */
	@Deprecated
	@Override
	public int executeUpdate(QueryParameters queryParameters, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException("Use executeReactiveUpdate instead ");
	}

	@Override
	public List<ParameterSpecification> getCollectedParameterSpecifications() {
		List<ParameterSpecification> parameterSpecifications = super.getCollectedParameterSpecifications();
		if ( parameterSpecifications == null ) {
			// Currently, ORM returns null for getCollectedParameterSpecifications() with a StatementExecutor
			// TODO: the following looks bad, I think it reparses the query
			SqlGenerator gen = new SqlGenerator( factory );
			try {
				gen.statement( (AST) getSqlAST() );
				return gen.getCollectedParameters();
			}
			catch (RecognitionException e) {
				throw QuerySyntaxException.convert(e);
			}
		}
		else {
			return parameterSpecifications;
		}
	}

	//TODO: Change scope in ORM
	private void errorIfSelect() throws HibernateException {
		if ( !getSqlAST().needsExecutor() ) {
			throw new QueryExecutionRequestException( "Not supported for select queries", getQueryString() );
		}
	}
}
