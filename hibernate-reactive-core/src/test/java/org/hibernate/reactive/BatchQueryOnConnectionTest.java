/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletionStage;
import javax.persistence.Entity;
import javax.persistence.Id;

import org.hibernate.cfg.Configuration;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.impl.PostgresParameters;
import org.hibernate.reactive.pool.impl.SQLServerParameters;

import org.junit.After;
import org.junit.Test;

import io.vertx.ext.unit.TestContext;

import static org.hibernate.reactive.containers.DatabaseConfiguration.dbType;

public class BatchQueryOnConnectionTest extends BaseReactiveTest {

	private static final int BATCH_SIZE = 20;

	@After
	public void cleanDb(TestContext context) {
		test( context, deleteEntities( "DataPoint" ) );
	}

	@Test
	public void testBatchInsertSizeEqMultiple(TestContext context) {
		final List<List<Object[]>> paramsBatches = doBatchInserts( context, 50, BATCH_SIZE );
		context.assertEquals( 3, paramsBatches.size() );
		context.assertEquals( 20, paramsBatches.get( 0 ).size() );
		context.assertEquals( 20, paramsBatches.get( 1 ).size() );
		context.assertEquals( 10, paramsBatches.get( 2 ).size() );
	}

	@Test
	public void testBatchInsertUpdateSizeLtMultiple(TestContext context) {
		final List<List<Object[]>> paramsBatches = doBatchInserts( context, 50, BATCH_SIZE - 1 );
		context.assertEquals( 3, paramsBatches.size() );
		context.assertEquals( 19, paramsBatches.get( 0 ).size() );
		context.assertEquals( 19, paramsBatches.get( 1 ).size() );
		context.assertEquals( 12, paramsBatches.get( 2 ).size() );
	}

	@Test
	public void testBatchInsertUpdateSizeGtMultiple(TestContext context) {
		final List<List<Object[]>> paramsBatches = doBatchInserts( context, 50, BATCH_SIZE + 1 );
		context.assertEquals( 5, paramsBatches.size() );
		context.assertEquals( 20, paramsBatches.get( 0 ).size() );
		context.assertEquals( 1, paramsBatches.get( 1 ).size() );
		context.assertEquals( 20, paramsBatches.get( 2 ).size() );
		context.assertEquals( 1, paramsBatches.get( 3 ).size() );
		context.assertEquals( 8, paramsBatches.get( 4 ).size() );
	}

	public List<List<Object[]>> doBatchInserts(TestContext context, int nEntities, int nEntitiesMultiple) {
		final String insertSql = process( "insert into DataPoint (description, x, y, id) values (?, ?, ?, ?)" );

		List<List<Object[]>> paramsBatches = new ArrayList<>();
		List<Object[]> paramsBatch = new ArrayList<>( BATCH_SIZE );

		for (int i = 1; i <= nEntities; i++) {

			DataPoint dp = new DataPoint(i);
			dp.description = "#" + i;
			dp.setX( new BigDecimal( i * 0.1d ).setScale( 19, RoundingMode.DOWN ) );
			dp.setY( new BigDecimal( dp.getX().doubleValue() * Math.PI ).setScale( 19, RoundingMode.DOWN ) );
			//uncomment to expose bug in DB2 client:
//			dp.setY(new BigDecimal(Math.cos(dp.getX().doubleValue())).setScale(19, BigDecimal.ROUND_DOWN));

			paramsBatch.add( new Object[] { dp.description, dp.x, dp.y, i } );

			if ( ( paramsBatch.size() == BATCH_SIZE ) || ( i % nEntitiesMultiple == 0 ) )  {
				paramsBatches.add( paramsBatch );
				paramsBatch = new ArrayList<>( BATCH_SIZE );
			}
		}

		if ( paramsBatch.size() > 0 ) {
			paramsBatches.add( paramsBatch );
		}

		CompletionStage<ReactiveConnection> stage = connection();
		for ( List<Object[]> batch : paramsBatches ) {
			stage = stage.thenCompose( connection -> connection.update( insertSql, batch )
					.thenApply( updateCounts -> {
						context.assertEquals( batch.size(), updateCounts.length );
						for ( int updateCount : updateCounts ) {
							context.assertEquals( 1, updateCount );
						}
						return connection;
					})
			);
		}

		test(
				context,
				stage.thenCompose( ignore -> openSession() )
				.thenCompose( s -> s.createQuery( "select count(*) from DataPoint" ).getSingleResult()
					.thenApply( result -> {
						context.assertEquals( (long) nEntities, result );
						return s;
					})
				)
		);

		return paramsBatches;
	}

	private String process(String sql) {
		switch ( dbType() ) {
			case POSTGRESQL:
			case COCKROACHDB:
				return PostgresParameters.INSTANCE.process( sql );
			case SQLSERVER:
				return SQLServerParameters.INSTANCE.process( sql );
			default:
				return sql;
		}
	}

	protected Configuration constructConfiguration() {
		Configuration configuration = super.constructConfiguration();
		configuration.addAnnotatedClass( DataPoint.class );
		return configuration;
	}

	@Entity(name = "DataPoint")
	public static class DataPoint {
		@Id
		private int id;
		private BigDecimal x;
		private BigDecimal y;
		private String description;

		public DataPoint() {
		}

		public DataPoint(int id) {
			this.id = id;
		}
		/**
		 * @return Returns the description.
		 */
		public String getDescription() {
			return description;
		}
		/**
		 * @param description The description to set.
		 */
		public void setDescription(String description) {
			this.description = description;
		}
		/**
		 * @return Returns the id.
		 */
		public int getId() {
			return id;
		}
		/**
		 * @param id The id to set.
		 */
		public void setId(int id) {
			this.id = id;
		}
		/**
		 * @return Returns the x.
		 */
		public BigDecimal getX() {
			return x;
		}
		/**
		 * @param x The x to set.
		 */
		public void setX(BigDecimal x) {
			this.x = x;
		}
		/**
		 * @return Returns the y.
		 */
		public BigDecimal getY() {
			return y;
		}
		/**
		 * @param y The y to set.
		 */
		public void setY(BigDecimal y) {
			this.y = y;
		}
	}
}
