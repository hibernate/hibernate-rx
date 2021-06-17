/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.dialect.CockroachDB201Dialect;
import org.hibernate.dialect.DB297Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDB103Dialect;
import org.hibernate.dialect.MySQL8Dialect;
import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.dialect.SQLServer2012Dialect;
import org.hibernate.reactive.containers.DatabaseConfiguration;
import org.hibernate.reactive.provider.Settings;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

public class UriConfigTest extends BaseReactiveTest {

    @Override
    protected Configuration constructConfiguration() {
        Class<? extends Dialect> dialect;
        switch ( dbType() ) {
            case POSTGRESQL: dialect = PostgreSQL10Dialect.class; break;
            case COCKROACHDB: dialect = CockroachDB201Dialect.class; break;
            case MYSQL: dialect = MySQL8Dialect.class; break;
            case MARIA: dialect = MariaDB103Dialect.class; break;
            case SQLSERVER: dialect = SQLServer2012Dialect.class; break;
            case DB2: dialect = DB297Dialect.class; break;
            default: throw new IllegalArgumentException("Database not recognized: " + dbType().name());
        }

        Configuration configuration = super.constructConfiguration();
        configuration.setProperty( Environment.DIALECT, dialect.getName() );
        configuration.setProperty( Settings.URL, DatabaseConfiguration.getUri() );
        configuration.setProperty( Settings.SQL_CLIENT_POOL_CONFIG, UriPoolConfiguration.class.getName() );
        return configuration;
    }

    @Test
    public void testUriConfig(TestContext context) {
        test( context, getSessionFactory()
                        .withTransaction( (s, t) -> s.createNativeQuery( selectQuery(), String.class ).getSingleResult() )
                        .thenAccept( context::assertNotNull )
        );
    }

    private String selectQuery() {
        switch ( dbType() ) {
            case POSTGRESQL:
            case COCKROACHDB:
            case SQLSERVER:
                return "select cast(current_timestamp as varchar)";
            case MARIA:
            case MYSQL:
                return "select cast(current_timestamp as char) from dual";
            case DB2:
				return "select cast(current time AS varchar) from sysibm.sysdummy1;";
            default:
                throw new IllegalArgumentException("Database not recognized: " + dbType().name());
        }
    }
}
