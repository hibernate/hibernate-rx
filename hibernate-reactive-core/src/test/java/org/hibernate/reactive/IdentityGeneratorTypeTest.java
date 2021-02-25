/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: LGPL-2.1-or-later
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.Rule;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;

/**
 * Test supported types for ids generated by the database
 */
public class IdentityGeneratorTypeTest extends BaseReactiveTest {

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( IntegerTypeEntity.class );
		configuration.addAnnotatedClass( LongTypeEntity.class );
		configuration.addAnnotatedClass( ShortTypeEntity.class );
		return configuration;
	}

	private <U extends Number, T extends TypeIdentity<U>> void assertType(
			TestContext context,
			Class<T> entityClass,
			T entity,
			U expectedId) {
		test( context, getMutinySessionFactory()
				.withSession( s -> s.persist( entity ).call( s::flush )
						.invoke( () -> {
							context.assertNotNull( entity.getId() );
							context.assertEquals( entity.getId(), expectedId );
						} ) )
				.chain( () -> getMutinySessionFactory()
						.withSession( s -> s.find( entityClass, entity.getId() )
								.invoke( result -> {
									context.assertNotNull( result );
									context.assertEquals( result.getId(), entity.getId() );
								} ) ) )
		);
	}

	@Test
	public void longIdentityType(TestContext context) {
		assertType( context, LongTypeEntity.class, new LongTypeEntity(), 1L );
	}

	@Test
	public void integerIdentityType(TestContext context) {
		assertType( context, IntegerTypeEntity.class, new IntegerTypeEntity(), 1 );
	}

	@Test
	public void shortIdentityType(TestContext context) {
		assertType( context, ShortTypeEntity.class, new ShortTypeEntity(), (short) 1 );
	}

	interface TypeIdentity<T extends Number> {
		T getId();
	}

	@Entity
	@Table(name = "IntegerTypeEntity")
	static class IntegerTypeEntity implements TypeIdentity<Integer> {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public Integer id;

		@Override
		public Integer getId() {
			return id;
		}
	}

	@Entity
	@Table(name = "LongTypeEntity")
	static class LongTypeEntity implements TypeIdentity<Long> {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public Long id;

		@Override
		public Long getId() {
			return id;
		}
	}

	@Entity
	@Table(name = "ShortTypeEntity")
	static class ShortTypeEntity implements TypeIdentity<Short> {
		@Id
		@GeneratedValue(strategy = GenerationType.IDENTITY)
		public Short id;

		@Override
		public Short getId() {
			return id;
		}
	}
}
