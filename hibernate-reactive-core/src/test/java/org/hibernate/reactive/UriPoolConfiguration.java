/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import io.vertx.db2client.DB2ConnectOptions;
import io.vertx.mssqlclient.MSSQLConnectOptions;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import org.hibernate.reactive.pool.impl.SqlClientPoolConfiguration;

import java.net.URI;

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

public class UriPoolConfiguration implements SqlClientPoolConfiguration {
    @Override
    public PoolOptions poolOptions() {
        return new PoolOptions();
    }

    @Override
    public SqlConnectOptions connectOptions(URI uri) {
        switch ( uri.getScheme() )
        {
            case "postgresql":
            case "postgres":
                return PgConnectOptions.fromUri( uri.toString() );
            case "mariadb":
            case "mysql":
                return MySQLConnectOptions.fromUri( uri.toString() );
            case "db2":
                return DB2ConnectOptions.fromUri( uri.toString() );
            case "sqlserver":
                return MSSQLConnectOptions.fromUri( uri.toString() );
            default: throw new IllegalArgumentException( "Database not recognized: " + dbType() );
        }
    }
}
