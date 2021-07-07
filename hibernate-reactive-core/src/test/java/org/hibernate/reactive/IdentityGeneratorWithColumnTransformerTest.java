/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.annotations.ColumnTransformer;
import org.hibernate.cfg.AvailableSettings;
import org.hibernate.cfg.Configuration;

import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

/**
 * Similar to {@link IdentityGeneratorTest} but enables SQL comments and uses a {@link ColumnTransformer}.
 */
public class IdentityGeneratorWithColumnTransformerTest extends BaseReactiveTest {

	// Comments don't work with Db2
	private static final String ENABLE_COMMENTS = String.valueOf( dbType() != DB2 );

	/**
	 * When {@link AvailableSettings#USE_GET_GENERATED_KEYS} is enabled, different
	 * queries will be used for each datastore to get the id
	 */
	public static class EnableUseGetGeneratedKeys extends IdentityGeneratorWithColumnTransformerTest {

		@Override
		protected Configuration constructConfiguration() {
			Configuration configuration = super.constructConfiguration();
			configuration.setProperty( AvailableSettings.USE_GET_GENERATED_KEYS, "true" );
			return configuration;
		}
	}

	// The number of entities we want to create
	private static final int ENTITY_NUMBER = 100;

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( EntityWithIdentity.class );
		configuration.setProperty( AvailableSettings.USE_SQL_COMMENTS, ENABLE_COMMENTS );
		return configuration;
	}

	private CompletionStage<?> populateDb(TestContext context) {
		final List<EntityWithIdentity> identities = new ArrayList<>( ENTITY_NUMBER );
		for ( int i = 0; i < ENTITY_NUMBER; i++ ) {
			identities.add( new EntityWithIdentity( i ) );
		}
		return getSessionFactory()
				.withTransaction( (session, tx) -> session.persist( identities.toArray() ) )
				.thenAccept( ignore -> {
					Long assignedId = 0L;
					for ( EntityWithIdentity identity : identities ) {
						context.assertNotNull( identity.id );
						context.assertTrue( identity.id > assignedId );
						assignedId = identity.id;
					}
				} );
	}

	@Test
	public void testIdentityGenerator(TestContext context) {
		test( context, populateDb( context )
				.thenCompose( v -> openSession() )
				.thenCompose( session ->
					  session.createQuery( "FROM EntityWithIdentity ORDER BY position ASC", EntityWithIdentity.class )
					  .getResultList() )
				.thenAccept( list -> {
					context.assertEquals( ENTITY_NUMBER, list.size() );
					int i = 0;
					for ( EntityWithIdentity entity : list ) {
						context.assertEquals( i * 2, entity.getPosition() );
						i++;
					}
				} ) );
	}

	@Entity(name = "EntityWithIdentity")
	@Table( name = "`Table with default values`")
	private static class EntityWithIdentity {
		private static final String PREFIX = "Entity: ";
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		Long id;

		@Column(unique = true)
		String name;

		@Column(name = "position")
		@ColumnTransformer(forColumn = "position", write = "? * 2")
		private int position;

		public EntityWithIdentity() {
		}

		public EntityWithIdentity(int index) {
			this.name =  PREFIX + index;
			this.position = index;
		}

		public int getPosition() {
			return position;
		}

		public void setPosition(int position) {
			this.position = position;
		}

		@Override
		public String toString() {
			return id + ":" + name + ":" + position;
		}
	}
}
