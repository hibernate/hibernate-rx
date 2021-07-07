/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.containers;

import org.testcontainers.containers.MSSQLServerContainer;

/**
 * The JDBC driver syntax is:
 *     jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]
 *
 * But the Vert.x SQL client syntax is:
 *     sqlserver://[user[:[password]]@]host[:port][/database][?attribute1=value1&attribute2=value2…​]
 */
class MSSQLServerDatabase implements TestableDatabase {

	public static final String IMAGE_NAME = "mcr.microsoft.com/mssql/server:2019-latest";

	public static final MSSQLServerDatabase INSTANCE = new MSSQLServerDatabase();

	public static final String PASSWORD = "~!HReact!~";

	/**
	 * Holds configuration for the Microsoft SQL Server database container. If the build is run with <code>-Pdocker</code> then
	 * Testcontainers+Docker will be used.
	 * <p>
	 * TIP: To reuse the same containers across multiple runs, set `testcontainers.reuse.enable=true` in a file located
	 * at `$HOME/.testcontainers.properties` (create the file if it does not exist).
	 */
	public static final MSSQLServerContainer<?> mssqlserver = new MSSQLServerContainer<>( IMAGE_NAME )
			.acceptLicense()
			.withPassword( PASSWORD )
			.withReuse( true );

	@Override
	public String getJdbcUrl() {
		return buildJdbcUrlWithCredentials( address() );
	}

	private String getRegularJdbcUrl() {
		return "jdbc:sqlserver://localhost:1433";
	}

	@Override
	public String getUri() {
		return buildUriWithCredentials( address() );
	}

	private String address() {
		if ( DatabaseConfiguration.USE_DOCKER ) {
			// Calling start() will start the container (if not already started)
			// It is required to call start() before obtaining the JDBC URL because it will contain a randomized port
			mssqlserver.start();
			return mssqlserver.getJdbcUrl();
		}

		return getRegularJdbcUrl();
	}

	private String buildJdbcUrlWithCredentials(String jdbcUrl) {
		return jdbcUrl + ";user=" + mssqlserver.getUsername() + ";password=" + mssqlserver.getPassword();
	}

	private static String buildUriWithCredentials(String jdbcUrl) {
		return "sqlserver://" + mssqlserver.getUsername() + ":" + mssqlserver.getPassword() + "@" + jdbcUrl.substring( "jdbc:sqlserver://".length() );
	}

	private MSSQLServerDatabase() {
	}

}
