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
import org.hibernate.reactive.session.ReactiveQueryExecutor;

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
     * Execute the statements using the query parameters
     */
    default CompletionStage<Integer> execute(ReactiveQueryExecutor session, QueryParameters queryParameters) {
        return total( 0, getSqlStatements().length, i -> {
            final Object[] arguments = QueryParametersAdaptor.arguments(
                    queryParameters,
                    getParameterSpecifications()[i],
                    session.getSharedContract()
            );
            return session.getReactiveConnection().update( getSqlStatements()[i], arguments );
        } );
    }
}
