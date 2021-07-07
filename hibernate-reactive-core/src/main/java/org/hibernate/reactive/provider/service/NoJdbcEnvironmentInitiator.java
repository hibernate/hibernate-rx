/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.provider.service;

import org.hibernate.HibernateException;
import org.hibernate.boot.registry.StandardServiceInitiator;
import org.hibernate.dialect.CockroachDB201Dialect;
import org.hibernate.dialect.DB297Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.MariaDB103Dialect;
import org.hibernate.dialect.MySQL8Dialect;
import org.hibernate.dialect.PostgreSQL10Dialect;
import org.hibernate.dialect.SQLServer2012Dialect;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.jdbc.dialect.spi.DatabaseMetaDataDialectResolutionInfoAdapter;
import org.hibernate.engine.jdbc.dialect.spi.DialectFactory;
import org.hibernate.engine.jdbc.env.internal.JdbcEnvironmentImpl;
import org.hibernate.engine.jdbc.env.internal.JdbcEnvironmentInitiator;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.service.spi.ServiceRegistryImplementor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 * A Hibernate {@link StandardServiceInitiator service initiator} that
 * provides an implementation of {@link JdbcEnvironment} that infers
 * the Hibernate {@link org.hibernate.dialect.Dialect} from the JDBC URL.
 */
public class NoJdbcEnvironmentInitiator extends JdbcEnvironmentInitiator {
	public static final NoJdbcEnvironmentInitiator INSTANCE = new NoJdbcEnvironmentInitiator();

	@Override
	public Class<JdbcEnvironment> getServiceInitiated() {
		return JdbcEnvironment.class;
	}

	@Override @SuppressWarnings("unchecked")
	public JdbcEnvironment initiateService(Map configurationValues, ServiceRegistryImplementor registry) {
		DialectFactory dialectFactory = registry.getService( DialectFactory.class );

		boolean explicitDialect = configurationValues.containsKey( Settings.DIALECT );
		String url = configurationValues.getOrDefault( Settings.URL, "" ).toString();
		if ( !explicitDialect ) {
			Class<? extends Dialect> dialectClass = guessDialect( url );
			if ( dialectClass != null ) {
				configurationValues.put( Settings.DIALECT, dialectClass.getName() );
				explicitDialect = true;
			}
		}

		// 'hibernate.temp.use_jdbc_metadata_defaults' is a temporary magic value.
		// The need for it is intended to be alleviated with future development, thus it is
		// not defined as an Settings constant...
		//
		// it is used to control whether we should consult the JDBC metadata to determine
		// certain Settings default values; it is useful to *not* do this when the database
		// may not be available (mainly in tools usage).
		boolean useJdbcMetadata = ConfigurationHelper.getBoolean(
				"hibernate.temp.use_jdbc_metadata_defaults",
				configurationValues,
				true
		);

		if ( useJdbcMetadata ) {
			ConnectionProvider connectionProvider = registry.getService( ConnectionProvider.class );
			final JdbcConnectionAccess jdbcConnectionAccess =
					new ConnectionProviderJdbcConnectionAccess( connectionProvider );
			try {
				final Connection connection = jdbcConnectionAccess.obtainConnection();
				try {
					Dialect dialect = dialectFactory.buildDialect(
							configurationValues,
							() -> {
								try {
									return new DatabaseMetaDataDialectResolutionInfoAdapter( connection.getMetaData() );
								}
								catch ( SQLException sqlException ) {
									return null;
								}
							}
					);
					return new JdbcEnvironmentImpl( registry, dialect, connection.getMetaData() );
				}
				catch (SQLException e) {}
				finally {
					try {
						jdbcConnectionAccess.releaseConnection( connection );
					}
					catch (SQLException ignore) {}
				}
			}
			catch (Exception e) {}
		}

		// if we get here, either we were asked to not use JDBC metadata or accessing the JDBC metadata failed.
		if ( explicitDialect ) {
			return new JdbcEnvironmentImpl( registry, dialectFactory.buildDialect( configurationValues, null ) );
		}
		else if ( url.isEmpty() ) {
			throw new HibernateException( "could not determine Dialect from JDBC driver metadata"
					+ " (specify a connection URI with scheme 'postgresql:', 'mysql:', 'cockroachdb', or 'db2:')" );
		}
		else {
			throw new HibernateException( "could not determine Dialect from connection URI '" + url
					+ "' (specify a connection URI with scheme 'postgresql:', 'mysql:', 'cockroachdb', or 'db2:')" );
		}
	}

	protected Class<? extends Dialect> guessDialect(String url) {
		if ( url.startsWith("jdbc:") ) {
			url = url.substring(5);
		}

		if ( url.startsWith("mysql:") ) {
			return MySQL8Dialect.class;
		}
		else if ( url.startsWith("mariadb:") ) {
			return MariaDB103Dialect.class;
		}
		else if ( url.startsWith("postgresql:") || url.startsWith("postgres:") ) {
			return PostgreSQL10Dialect.class;
		}
		else if ( url.startsWith("db2:") ) {
			return  DB297Dialect.class;
		}
		else if ( url.startsWith("cockroachdb:") ) {
			return  CockroachDB201Dialect.class;
		}
		else if ( url.startsWith("sqlserver:") ) {
			return  SQLServer2012Dialect.class;
		}
		else {
			return null;
		}
	}
}
