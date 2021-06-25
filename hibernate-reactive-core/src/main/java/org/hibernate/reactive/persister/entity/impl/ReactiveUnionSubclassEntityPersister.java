/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.persister.entity.impl;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.util.concurrent.CompletionStage;

import org.hibernate.HibernateException;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.MappingException;
import org.hibernate.cache.spi.access.EntityDataAccess;
import org.hibernate.cache.spi.access.NaturalIdDataAccess;
import org.hibernate.engine.spi.CascadingActions;
import org.hibernate.engine.spi.EntityEntry;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.id.IdentifierGenerator;
import org.hibernate.id.IdentityGenerator;
import org.hibernate.jdbc.Expectation;
import org.hibernate.loader.entity.UniqueEntityLoader;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.persister.entity.UnionSubclassEntityPersister;
import org.hibernate.persister.spi.PersisterCreationContext;
import org.hibernate.reactive.loader.entity.ReactiveUniqueEntityLoader;
import org.hibernate.reactive.loader.entity.impl.ReactiveBatchingEntityLoaderBuilder;
import org.hibernate.reactive.loader.entity.impl.ReactiveCascadeEntityLoader;
import org.hibernate.type.Type;

/**
 * An {@link ReactiveEntityPersister} backed by {@link UnionSubclassEntityPersister}
 * and {@link ReactiveAbstractEntityPersister}.
 */
public class ReactiveUnionSubclassEntityPersister extends UnionSubclassEntityPersister
		implements ReactiveAbstractEntityPersister {

	private String sqlUpdateGeneratedValuesSelectString;
	private String sqlInsertGeneratedValuesSelectString;

	public ReactiveUnionSubclassEntityPersister(
			PersistentClass persistentClass,
			EntityDataAccess cacheAccessStrategy,
			NaturalIdDataAccess naturalIdRegionAccessStrategy,
			PersisterCreationContext creationContext) throws HibernateException {
		super( persistentClass, cacheAccessStrategy, naturalIdRegionAccessStrategy, creationContext );
	}

	@Override
	public String generateSelectVersionString() {
		String sql = super.generateSelectVersionString();
		return parameters().process( sql );
	}

	@Override
	public String generateUpdateGeneratedValuesSelectString() {
		sqlUpdateGeneratedValuesSelectString = parameters()
				.process( super.generateUpdateGeneratedValuesSelectString() );
		return sqlUpdateGeneratedValuesSelectString;
	}

	@Override
	public String generateInsertGeneratedValuesSelectString() {
		sqlInsertGeneratedValuesSelectString = parameters()
				.process( super.generateInsertGeneratedValuesSelectString() );
		return sqlInsertGeneratedValuesSelectString;
	}
	@Override
	public String generateSnapshotSelectString() {
		String sql = super.generateSnapshotSelectString();
		return parameters().process( sql );
	}

	@Override
	public String generateDeleteString(int j) {
		String sql = super.generateDeleteString( j );
		return parameters().process( sql );
	}

	@Override
	public String generateUpdateString(boolean[] includeProperty, int j, boolean useRowId) {
		String sql = super.generateUpdateString( includeProperty, j, useRowId );
		return parameters().process( sql );
	}

	@Override
	public String generateUpdateString(boolean[] includeProperty, int j, Object[] oldFields, boolean useRowId) {
		String sql = super.generateUpdateString( includeProperty, j, oldFields, useRowId );
		return parameters().process( sql );
	}

	@Override
	public String generateInsertString(boolean[] includeProperty, int j) {
		String sql = super.generateInsertString( includeProperty, j );
		return  parameters().process( sql, includeProperty.length );
	}

	@Override
	public String generateInsertString(boolean identityInsert, boolean[] includeProperty) {
		String sql =  super.generateInsertString( identityInsert, includeProperty );
		return parameters().process( sql, includeProperty.length );
	}

	@Override
	public String generateInsertString(boolean identityInsert, boolean[] includeProperty, int j) {
		String sql =  super.generateInsertString( identityInsert, includeProperty, j );
		return parameters().process( sql, includeProperty.length );
	}

	@Override
	public String generateIdentityInsertString(boolean[] includeProperty) {
		String sql =  super.generateIdentityInsertString( includeProperty );
		return parameters().process( sql, includeProperty.length );
	}

	@Override
	public IdentifierGenerator getIdentifierGenerator() throws HibernateException {
		final IdentifierGenerator identifierGenerator = super.getIdentifierGenerator();
		if ( identifierGenerator instanceof IdentityGenerator ) {
			return new ReactiveIdentityGenerator();
		}
		return identifierGenerator;
	}

	@Override
	public boolean hasProxy() {
		return hasUnenhancedProxy();
	}

	@Override
	protected UniqueEntityLoader buildMergeCascadeEntityLoader(LockMode ignored) {
		return new ReactiveCascadeEntityLoader( this, CascadingActions.MERGE, getFactory() );
	}

	@Override
	protected UniqueEntityLoader buildRefreshCascadeEntityLoader(LockMode ignored) {
		return new ReactiveCascadeEntityLoader( this, CascadingActions.REFRESH, getFactory() );
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockMode lockMode, LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		return ReactiveBatchingEntityLoaderBuilder.getBuilder( getFactory() )
				.buildLoader( this, batchSize, lockMode, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected UniqueEntityLoader createEntityLoader(LockOptions lockOptions, LoadQueryInfluencers loadQueryInfluencers)
			throws MappingException {
		return ReactiveBatchingEntityLoaderBuilder.getBuilder( getFactory() )
				.buildLoader( this, batchSize, lockOptions, getFactory(), loadQueryInfluencers );
	}

	@Override
	protected UniqueEntityLoader createUniqueKeyLoader(Type uniqueKeyType, String[] columns, LoadQueryInfluencers loadQueryInfluencers) {
		return createReactiveUniqueKeyLoader(uniqueKeyType, columns, loadQueryInfluencers);
	}

	@Override
	public Serializable insert(
			Object[] fields, boolean[] notNull, String sql, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void insert(
			Serializable id,
			Object[] fields,
			boolean[] notNull,
			int j,
			String sql,
			Object object,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public Serializable insert(
			Object[] fields, Object object, SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void insert(Serializable id, Object[] fields, Object object, SharedSessionContractImplementor session) {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void delete(
			Serializable id,
			Object version,
			int j,
			Object object,
			String sql,
			SharedSessionContractImplementor session,
			Object[] loadedState) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void delete(
			Serializable id, Object version, Object object, SharedSessionContractImplementor session)
			throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void updateOrInsert(
			Serializable id,
			Object[] fields,
			Object[] oldFields,
			Object rowId,
			boolean[] includeProperty,
			int j,
			Object oldVersion,
			Object object,
			String sql,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public boolean update(
			Serializable id,
			Object[] fields,
			Object[] oldFields,
			Object rowId,
			boolean[] includeProperty,
			int j,
			Object oldVersion,
			Object object,
			String sql,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public void update(
			Serializable id,
			Object[] fields,
			int[] dirtyFields,
			boolean hasDirtyCollection,
			Object[] oldFields,
			Object oldVersion,
			Object object,
			Object rowId,
			SharedSessionContractImplementor session) throws HibernateException {
		throw new UnsupportedOperationException( "Wrong method calls. Use the reactive equivalent." );
	}

	@Override
	public ReactiveUniqueEntityLoader getAppropriateLoader(LockOptions lockOptions, SharedSessionContractImplementor session) {
		return (ReactiveUniqueEntityLoader) super.getAppropriateLoader(lockOptions, session);
	}

	@Override
	public String getSqlInsertGeneratedValuesSelectString() {
		return sqlInsertGeneratedValuesSelectString;
	}

	@Override
	public String getSqlUpdateGeneratedValuesSelectString() {
		return sqlUpdateGeneratedValuesSelectString;
	}

	public ReactiveUniqueEntityLoader getAppropriateUniqueKeyLoader(String propertyName, SharedSessionContractImplementor session) {
		return (ReactiveUniqueEntityLoader) super.getAppropriateUniqueKeyLoader(propertyName, session);
	}

	@Override
	public void preInsertInMemoryValueGeneration(Object[] fields, Object object,
												 SharedSessionContractImplementor session) {
		super.preInsertInMemoryValueGeneration(fields, object, session);
	}

	@Override
	public String[] getUpdateStrings(boolean byRowId, boolean hasUninitializedLazyProperties) {
		return super.getUpdateStrings(byRowId, hasUninitializedLazyProperties);
	}

	@Override
	public boolean check(
			int rows, Serializable id, int tableNumber,
			Expectation expectation, PreparedStatement statement, String sql) throws HibernateException {
		return super.check(rows, id, tableNumber, expectation, statement, sql);
	}

	@Override
	public boolean initializeLazyProperty(String fieldName, Object entity,
										  SharedSessionContractImplementor session,
										  EntityEntry entry,
										  int lazyIndex,
										  Object selectedValue) {
		return super.initializeLazyProperty(fieldName, entity, session, entry, lazyIndex, selectedValue);
	}

	@Override
	public CompletionStage<Object> initializeLazyPropertiesFromDatastore(String fieldName, Object entity,
																	   SharedSessionContractImplementor session,
																	   Serializable id, EntityEntry entry) {
		return reactiveInitializeLazyPropertiesFromDatastore(fieldName, entity, session, id, entry);
	}

	@Override
	public String[][] getLazyPropertyColumnAliases() {
		return super.getLazyPropertyColumnAliases();
	}

	@Override
	public String determinePkByNaturalIdQuery(boolean[] valueNullness) {
		return super.determinePkByNaturalIdQuery(valueNullness);
	}
}
