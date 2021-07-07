/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import org.hibernate.dialect.CockroachDB192Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.PostgreSQL9Dialect;
import org.hibernate.dialect.SQLServerDialect;

/**
 * Some databases have a different parameter syntax, which
 * the Vert.x {@link io.vertx.sqlclient.SqlClient} does not abstract.
 * This class converts JDBC/ODBC-style {@code ?} parameters generated
 * by Hibernate ORM to the native format.
 */
public abstract class Parameters {

	private static final Parameters NO_PARSING = new Parameters() {
		@Override
		public String process(String sql) {
			return sql;
		}

		@Override
		public String process(String sql, int parameterCount) {
			return sql;
		}

		@Override
		public String processLimit(String sql, Object[] parameterArray, boolean hasOffset) {
			return sql;
		}
	};

	public static Parameters instance(Dialect dialect) {
		if (dialect instanceof PostgreSQL9Dialect || dialect instanceof CockroachDB192Dialect) return PostgresParameters.INSTANCE;
		if (dialect instanceof SQLServerDialect) return SQLServerParameters.INSTANCE;
		return NO_PARSING;
	}

	public static boolean isProcessingNotRequired(String sql) {
		return sql == null
				// There aren't any parameters
				|| sql.indexOf('?') == -1;
	}

	public abstract String process(String sql);

	public abstract String process(String sql, int parameterCount);

	public abstract String processLimit(String sql, Object[] parameterArray, boolean hasOffset);
}
