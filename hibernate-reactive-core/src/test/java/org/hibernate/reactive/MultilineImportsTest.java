/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.List;
import java.util.concurrent.CompletionStage;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;

import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.provider.Settings;
import org.hibernate.reactive.stage.Stage;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;


/**
 * Test importing a SQL script which is using the multi-line format
 */
public class MultilineImportsTest extends BaseReactiveTest {

	private static CompletionStage<List<Object>> runQuery(Stage.Session s) {
		return s.createQuery( "from Hero h where h.name = :name" )
				.setParameter( "name", "Galadriel" )
				.getResultList();
	}

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		// Testing the model from the Quarkus workshops (a smaller extract of):
		// https://github.com/quarkusio/quarkus-workshops/tree/master/quarkus-workshop-super-heroes/super-heroes/rest-hero
		configuration.setProperty( AvailableSettings.HBM2DDL_IMPORT_FILES, "/complexMultilineImports.sql" );
		configuration.setProperty( Settings.HBM2DDL_AUTO, "create" );
		configuration.setProperty( Settings.SHOW_SQL, "true" );
		configuration.setProperty( Settings.HBM2DDL_IMPORT_FILES_SQL_EXTRACTOR, "org.hibernate.tool.hbm2ddl.MultipleLinesSqlCommandExtractor" );
		configuration.addAnnotatedClass( Hero.class );
		return configuration;
	}

	@Test
	public void verifyImportScriptHasRun(TestContext context) {
		test(
				context,
				getSessionFactory()
						.withSession( MultilineImportsTest::runQuery )
						.thenAccept( list -> context.assertEquals( list.size(), 1) )
		);
	}

	@Entity(name = "Hero")
	@Table(name = "hero")
	public static class Hero {

		@javax.persistence.Id
		@javax.persistence.GeneratedValue
		public java.lang.Long id;

		@Column(unique = true)
		public String name;

		public String otherName;

		public int level;

		public String picture;

		public String powers;

	}

}
