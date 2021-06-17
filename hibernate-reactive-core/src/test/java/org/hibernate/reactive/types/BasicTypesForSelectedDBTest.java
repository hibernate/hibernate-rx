/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.types;

import java.util.Objects;
import java.util.function.Consumer;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Lob;
import javax.persistence.Table;
import javax.persistence.Version;

import org.hibernate.annotations.Type;
import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.BaseReactiveTest;
import org.hibernate.reactive.testing.DatabaseSelectionRule;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.DB2;
import static org.hibernate.reactive.containers.DatabaseConfiguration.DBType.SQLSERVER;

/**
 * Test types that we expect to work only on selected DBs.
 */
public class BasicTypesForSelectedDBTest extends BaseReactiveTest {

	@Rule
	public DatabaseSelectionRule selectionRule = DatabaseSelectionRule.skipTestsFor( DB2, SQLSERVER );

	@Override
	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( Basic.class );
		return configuration;
	}

	@After
	public void deleteTable(TestContext context) {
		test( context,
			  getSessionFactory().withSession(
					  session -> session.createQuery( "delete from Basic" ).executeUpdate()
			  )
		);
	}

	@Test
	public void testStringLobType(TestContext context) {
		String text = "hello world once upon a time it was the best of times it was the worst of times goodbye";
		StringBuilder longText = new StringBuilder();
		for ( int i = 0; i < 1000; i++ ) {
			longText.append( text );
		}
		String book = longText.toString();

		Basic basic = new Basic();
		basic.book = book;

		testField( context, basic, found -> context.assertEquals( book, found.book ) );
	}

	@Test
	public void testBytesLobType(TestContext context) {
		String text = "hello world once upon a time it was the best of times it was the worst of times goodbye";
		StringBuilder longText = new StringBuilder();
		for ( int i = 0; i < 1000; i++ ) {
			longText.append( text );
		}
		byte[] pic = longText.toString().getBytes();

		Basic basic = new Basic();
		basic.pic = pic;
		testField( context, basic, found -> context.assertTrue( Objects.deepEquals( pic, found.pic ) ) );
	}

	@Test
	public void testJsonType(TestContext context) {
		Basic basic = new Basic();
		basic.jsonObj = new JsonObject().put("int", 123).put("str", "hello");

		testField( context, basic, found -> context.assertEquals( basic.jsonObj, found.jsonObj) );
	}

	/**
	 * Persist the entity, find it and execute the assertions
	 */
	private void testField(TestContext context, Basic original, Consumer<Basic> consumer) {
		test(
				context,
				getSessionFactory().withTransaction( (s, t) -> s.persist( original ) )
						.thenCompose( v -> openSession() )
						.thenCompose( s2 -> s2.find( Basic.class,  original.id )
								.thenAccept( found -> {
									context.assertNotNull( found );
									context.assertEquals( original, found );
									consumer.accept( found );
								} ) )
		);
	}

	@Entity(name="Basic") @Table(name="Basic")
	private static class Basic {

		@Id @GeneratedValue Integer id;
		@Version Integer version;
		String string;

		@Lob @Column(length = 100_000) protected byte[] pic;
		@Lob @Column(length = 100_000) protected String book;

		@Type(type="org.hibernate.reactive.types.Json")
		@Column(columnDefinition = "json")
		private JsonObject jsonObj;

		public Basic() {
		}

		public Basic(String string) {
			this.string = string;
		}

		public Basic(String string, byte[] pic, String book) {
			this.string = string;
			this.pic = pic;
			this.book = book;
		}

		public Basic(Integer id, String string) {
			this.id = id;
			this.string = string;
		}

		public Integer getId() {
			return id;
		}

		public void setId(Integer id) {
			this.id = id;
		}

		@Override
		public String toString() {
			return id + ": " + string;
		}

		@Override
		public boolean equals(Object o) {
			if ( this == o ) {
				return true;
			}
			if ( o == null || getClass() != o.getClass() ) {
				return false;
			}
			Basic basic = (Basic) o;
			return Objects.equals(string, basic.string);
		}

		@Override
		public int hashCode() {
			return Objects.hash(string);
		}
	}
}
