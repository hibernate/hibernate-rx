/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.adaptor.impl;

import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.param.ParameterSpecification;
import org.hibernate.type.Type;


public final class QueryParametersAdaptor {

	private QueryParametersAdaptor() {
		//do not construct
	}

	public static Object[] arguments(QueryParameters queryParameters,
									 SharedSessionContractImplementor session,
									 LimitHandler limitHandler) {
		return PreparedStatementAdaptor.bind( adaptor -> {
			Type[] types = queryParameters.getFilteredPositionalParameterTypes();
			Object[] values = queryParameters.getFilteredPositionalParameterValues();
			int pos = 1;
			for (int i = 0; i < types.length; i++) {
				Type type = types[i];
				Object value = values[i];
				type.nullSafeSet( adaptor, value, pos, session );
				pos += type.getColumnSpan( session.getFactory() );
			}

			pos += limitHandler.bindLimitParametersAtEndOfQuery( queryParameters.getRowSelection(), adaptor, pos );
		} );
	}

	public static Object[] arguments(
			QueryParameters queryParameters,
			ParameterSpecification[] parameterSpecifications,
			SharedSessionContractImplementor session) {
		return PreparedStatementAdaptor.bind( adaptor -> {
			int pos = 1;
			for (ParameterSpecification parameterSpecification: parameterSpecifications) {
				pos += parameterSpecification.bind( adaptor, queryParameters, session, pos );
			}
		} );
	}
}
