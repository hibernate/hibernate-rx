/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bulk;

import java.util.concurrent.CompletionStage;

import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.param.ParameterSpecification;
import org.hibernate.reactive.adaptor.impl.QueryParametersAdaptor;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.session.ReactiveQueryExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;

import static org.hibernate.reactive.util.impl.CompletionStages.total;


/**
 * A list of SQL statements to be executed as a single logical unit.
 * May include both DDL and DML statements.
 *
 * @author Gavin King
 */
public interface StatementsWithParameters {

    /**
     * The SQL statements to be executed.
     */
    String[] getSqlStatements();

    /**
     * The parameters of the corresponding SQL statements.
     */
    ParameterSpecification[][] getParameterSpecifications();

    /**
     * Is the given statement executed inside the current transaction?
     *
     * @return true by default
     */
    default boolean isTransactionalStatement(String statement) {
        return true;
    }

    /**
     * Should the result of this statement contribute to the running
     * updated row count?
     *
     * @return false for DDL statements by default
     */
    default boolean  isSchemaDefinitionStatement(String statement) {
        return statement.startsWith("create ")
            || statement.startsWith("drop ");
    }

    /**
     * Execute the statements using the query parameters
     */
    default CompletionStage<Integer> execute(ReactiveQueryExecutor session, QueryParameters queryParameters) {
        return total( 0, getSqlStatements().length, i -> {
            final String sql = getSqlStatements()[i];
            ReactiveConnection connection = session.getReactiveConnection();
            if ( !isSchemaDefinitionStatement( sql ) ) {
                final Object[] arguments = QueryParametersAdaptor.arguments(
                        queryParameters,
                        getParameterSpecifications()[i],
                        session.getSharedContract()
                );
                return connection.update( sql, arguments );
            }
            else if ( isTransactionalStatement( sql ) ) {
                // a DML statement that should be executed within the
                // transaction (local temporary tables)
                return connection.execute( sql )
                        .thenCompose( CompletionStages::zeroFuture );
            }
            else {
                // a DML statement that should be executed outside the
                // transaction (global temporary tables)
                return connection.executeOutsideTransaction( sql )
                        // ignore errors creating tables, since a create
                        // table fails whenever the table already exists
                        .handle( (v, x) -> 0 );
            }
        } );
    }
}
