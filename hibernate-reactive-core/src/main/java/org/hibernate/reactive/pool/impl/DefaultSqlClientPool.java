/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.pool.impl;

import java.net.URI;
import java.util.Map;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateError;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.jdbc.spi.SqlStatementLogger;
import org.hibernate.internal.util.config.ConfigurationException;
import org.hibernate.internal.util.config.ConfigurationHelper;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.vertx.VertxInstance;
import org.hibernate.service.spi.Configurable;
import org.hibernate.service.spi.ServiceRegistryAwareService;
import org.hibernate.service.spi.ServiceRegistryImplementor;
import org.hibernate.service.spi.Startable;
import org.hibernate.service.spi.Stoppable;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import io.vertx.sqlclient.spi.Driver;

import static java.util.Collections.singletonList;

import static org.hibernate.internal.CoreLogging.messageLogger;

/**
 * A pool of reactive connections backed by a Vert.x {@link Pool}.
 * The {@code Pool} itself is backed by an instance of {@link Vertx}
 * obtained via the {@link VertxInstance} service. Configuration of
 * the {@code Pool} is handled by the {@link SqlClientPoolConfiguration}
 * service.
 * <p>
 * This class may be extended by programs which wish to implement
 * custom connection management or multitenancy.
 * <p>
 * The lifecycle of this pool is managed by Hibernate Reactive: it
 * is created when the reactive {@link org.hibernate.SessionFactory}
 * is created and destroyed when the {@code SessionFactory} is
 * destroyed. For cases where the underlying {@code Pool} lifecycle
 * is managed externally to Hibernate, use
 * {@link org.hibernate.reactive.pool.impl.ExternalSqlClientPool}.
 *
 * @see SqlClientPoolConfiguration
 */
public class DefaultSqlClientPool extends SqlClientPool
		implements ServiceRegistryAwareService, Configurable, Stoppable, Startable {

	private enum VertxDriver {
		DB2( "io.vertx.db2client.spi.DB2Driver", "db2" ),
		MYSQL( "io.vertx.mysqlclient.spi.MySQLDriver", "mysql", "mariadb" ),
		POSTGRES( "io.vertx.pgclient.spi.PgDriver", "postgres", "postgre", "postgresql", "cockroachdb" ),
		MSSQL( "io.vertx.mssqlclient.spi.MSSQLDriver", "sqlserver" );

		private final String className;
		private final String[] schemas;

		VertxDriver(String className, String... schemas) {
			this.className = className;
			this.schemas = schemas;
		}

		/**
		 * Does the driver support the schema?
		 *
		 * @param schema the url schema
		 * @return true if the driver supports the schema, false otherwise
		 */
		public boolean matches(String schema) {
			for ( String alias : schemas ) {
				if ( alias.equalsIgnoreCase( schema ) ) {
					return true;
				}
			}
			return false;
		}

		/**
		 * Find the Vert.x driver for the given class name.
		 *
		 * @param className the canonical name of the driver class
		 * @return a {@link VertxDriver} or null
		 */
		public static VertxDriver findByClassName(String className) {
			for ( VertxDriver driver : values() ) {
				if ( driver.className.equalsIgnoreCase( className ) ) {
					return driver;
				}
			}
			return null;
		}
	}

	private Pool pools;
	private SqlStatementLogger sqlStatementLogger;
	private URI uri;
	private ServiceRegistryImplementor serviceRegistry;

	//Asynchronous shutdown promise: we can't return it from #close as we implement a
	//blocking interface.
	private volatile Future<Void> closeFuture = Future.succeededFuture();

	public DefaultSqlClientPool() {}

	@Override
	public void injectServices(ServiceRegistryImplementor serviceRegistry) {
		this.serviceRegistry = serviceRegistry;
		sqlStatementLogger = serviceRegistry.getService(JdbcServices.class).getSqlStatementLogger();
	}

	@Override
	public void configure(Map configuration) {
		uri = jdbcUrl( configuration );
	}

	@Override
	public void start() {
		if ( pools == null ) {
			pools = createPool( uri );
		}
	}

	@Override
	public CompletionStage<Void> getCloseFuture() {
		return closeFuture.toCompletionStage();
	}

	@Override
	protected Pool getPool() {
		return pools;
	}

	@Override
	protected SqlStatementLogger getSqlStatementLogger() {
		return sqlStatementLogger;
	}

	/**
	 * Create a new {@link Pool} for the given JDBC URL or database URI,
	 * using the {@link VertxInstance} service to obtain an instance of
	 * {@link Vertx}, and the {@link SqlClientPoolConfiguration} service
	 * to obtain options for creating the connection pool instances.
	 *
	 * @param uri JDBC URL or database URI
	 *
	 * @return the new {@link Pool}
	 */
	protected Pool createPool(URI uri) {
		SqlClientPoolConfiguration configuration = serviceRegistry.getService(SqlClientPoolConfiguration.class);
		VertxInstance vertx = serviceRegistry.getService(VertxInstance.class);
		return createPool( uri, configuration.connectOptions( uri ), configuration.poolOptions(), vertx.getVertx() );
	}

	/**
	 * Create a new {@link Pool} for the given JDBC URL or database URI,
	 * connection pool options, and the given instance of {@link Vertx}.
	 *
	 * @param uri JDBC URL or database URI
	 * @param connectOptions the connection options
	 * @param poolOptions the connection pooling options
	 * @param vertx the instance of {@link Vertx} to be used by the pool
	 *
	 * @return the new {@link Pool}
	 */
	protected Pool createPool(URI uri, SqlConnectOptions connectOptions, PoolOptions poolOptions, Vertx vertx) {
		try {
			// First try to load the Pool using the standard ServiceLoader pattern
			// This only works if exactly 1 Driver is on the classpath.
			return Pool.pool( vertx, connectOptions, poolOptions );
		}
		catch (ServiceConfigurationError e) {
			// Backup option if multiple drivers are on the classpath.
			// We will be able to remove this once Vertx 3.9.2 is available
			final Driver driver = findDriver( uri, e );
			return driver.createPool( vertx, singletonList( connectOptions ), poolOptions );
		}
	}

	/**
	 * Determine the JDBC URL or database URI from the given configuration.
	 *
	 * @param configurationValues the configuration properties
	 *
	 * @return the JDBC URL as a {@link URI}
	 */
	protected URI jdbcUrl(Map<?,?> configurationValues) {
		String url = ConfigurationHelper.getString( Settings.URL, configurationValues );
		messageLogger( DefaultSqlClientPool.class ).infof( "HRX000011: SQL Client URL [%s]", url );
		return parse( url );
	}

	/**
	 * When there are multiple candidate drivers in the classpath,
	 * {@link Pool#pool} throws a {@link ServiceConfigurationError},
	 * so we need to disambiguate according to the scheme specified
	 * in the given {@link URI}.
	 *
	 * @param uri the JDBC URL or database URI
	 * @param originalError the error that was thrown
	 *
	 * @return the disambiguated {@link Driver}
	 */
	private Driver findDriver(URI uri, ServiceConfigurationError originalError) {
		String scheme = uri.getScheme(); // "postgresql", "mysql", "db2", etc
		for ( Driver d : ServiceLoader.load( Driver.class ) ) {
			String driverName = d.getClass().getCanonicalName();
			messageLogger( DefaultSqlClientPool.class ).infof( "HRX000013: Detected driver [%s]", driverName );
			if ( matchesScheme( driverName, scheme ) ) {
				return d;
			}
		}
		throw new ConfigurationException( "No suitable drivers found for URI scheme: " + uri.getScheme(), originalError );
	}

	private boolean matchesScheme(String driverName, String scheme) {
		VertxDriver vertxDriver = VertxDriver.findByClassName( driverName );
		return vertxDriver != null && vertxDriver.matches( scheme );
	}

	@Override
	public void stop() {
		if ( pools != null ) {
			this.closeFuture = pools.close();
		}
	}

	public static URI parse(String url) {

		if ( url == null || url.trim().isEmpty() ) {
			throw new HibernateError( "The configuration property '" + Settings.URL + "' was not provided, or is in invalid format. This is required when using the default DefaultSqlClientPool: " +
											  "either provide the configuration setting or integrate with a different SqlClientPool implementation" );
		}

		if ( url.startsWith( "jdbc:" ) ) {
			return URI.create( updateUrl( url.substring( 5 ) ) );
		}

		return URI.create( updateUrl( url ) );
	}

	private static String updateUrl(String url) {
		return url.replaceAll( "^cockroachdb:", "postgres:" );
	}

}
