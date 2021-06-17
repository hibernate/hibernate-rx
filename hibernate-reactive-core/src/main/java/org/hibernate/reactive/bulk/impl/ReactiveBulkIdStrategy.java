/* Hibernate, Relational Persistence for Idiomatic Java
 *
 * SPDX-License-Identifier: Apache-2.0
 * Copyright: Red Hat Inc. and Hibernate Authors
 */
package org.hibernate.reactive.bulk.impl;

import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.spi.MetadataBuildingOptions;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.boot.spi.SessionFactoryOptions;
import org.hibernate.dialect.DB297Dialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.config.spi.ConfigurationService;
import org.hibernate.engine.config.spi.StandardConverters;
import org.hibernate.engine.jdbc.connections.spi.JdbcConnectionAccess;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.QueryParameters;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.engine.spi.SharedSessionContractImplementor;
import org.hibernate.hql.internal.ast.HqlSqlWalker;
import org.hibernate.hql.internal.ast.tree.AbstractRestrictableStatement;
import org.hibernate.hql.internal.ast.tree.AssignmentSpecification;
import org.hibernate.hql.internal.ast.tree.DeleteStatement;
import org.hibernate.hql.internal.ast.tree.FromElement;
import org.hibernate.hql.internal.ast.tree.UpdateStatement;
import org.hibernate.hql.spi.id.AbstractMultiTableBulkIdStrategyImpl;
import org.hibernate.hql.spi.id.AbstractTableBasedBulkIdHandler;
import org.hibernate.hql.spi.id.IdTableInfo;
import org.hibernate.hql.spi.id.MultiTableBulkIdStrategy;
import org.hibernate.hql.spi.id.global.GlobalTemporaryTableBulkIdStrategy;
import org.hibernate.hql.spi.id.local.IdTableInfoImpl;
import org.hibernate.internal.CoreLogging;
import org.hibernate.internal.CoreMessageLogger;
import org.hibernate.internal.util.collections.ArrayHelper;
import org.hibernate.mapping.PersistentClass;
import org.hibernate.mapping.Table;
import org.hibernate.param.ParameterSpecification;
import org.hibernate.persister.collection.AbstractCollectionPersister;
import org.hibernate.persister.entity.Queryable;
import org.hibernate.reactive.bulk.StatementsWithParameters;
import org.hibernate.reactive.pool.ReactiveConnection;
import org.hibernate.reactive.pool.ReactiveConnectionPool;
import org.hibernate.reactive.pool.impl.Parameters;
import org.hibernate.reactive.session.ReactiveQueryExecutor;
import org.hibernate.reactive.util.impl.CompletionStages;
import org.hibernate.sql.Delete;
import org.hibernate.sql.Update;
import org.hibernate.type.CollectionType;
import org.hibernate.type.Type;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

import static org.hibernate.reactive.util.impl.CompletionStages.total;

/**
 * A reactive version of {@link AbstractMultiTableBulkIdStrategyImpl} used
 * for handling HQL update and delete queries that affect multiple tables.
 * <p>
 * Note that this class features hardcoded support for the three supported
 * databases.
 *
 * @author Gavin King
 */
public class ReactiveBulkIdStrategy
		extends AbstractMultiTableBulkIdStrategyImpl<IdTableInfoImpl, AbstractMultiTableBulkIdStrategyImpl.PreparationContext>
		implements MultiTableBulkIdStrategy {

	private static final CoreMessageLogger LOG = CoreLogging.messageLogger( ReactiveBulkIdStrategy.class );

	private static final ParameterSpecification[] NO_PARAMS = new ParameterSpecification[0];

	private final boolean db2;
	private final Set<String> createdGlobalTemporaryTables = new HashSet<>();
	private final List<String> dropTableStatements = new ArrayList<>();
	private final Parameters parameters;

	private StandardServiceRegistry serviceRegistry;

//	private boolean useSessionIdColumn() {
//		return db2;
//	}

	public ReactiveBulkIdStrategy(MetadataImplementor metadata) {
		this( metadata.getDatabase().getDialect() );
	}

	ReactiveBulkIdStrategy(Dialect dialect) {
		super( new ReactiveIdTableSupport( dialect ) );
		db2 = dialect instanceof DB297Dialect;
		parameters = Parameters.instance( dialect );
	}

	@Override
	protected void initialize(MetadataBuildingOptions buildingOptions, SessionFactoryOptions sessionFactoryOptions) {
		serviceRegistry = buildingOptions.getServiceRegistry();
	}

	@Override
	protected IdTableInfoImpl buildIdTableInfo(
			PersistentClass entityBinding,
			Table idTable,
			JdbcServices jdbcServices,
			MetadataImplementor metadata,
			PreparationContext context) {
		return new IdTableInfoImpl(
				jdbcServices.getJdbcEnvironment().getQualifiedObjectNameFormatter().format(
						idTable.getQualifiedTableName(),
						jdbcServices.getJdbcEnvironment().getDialect()
				),
				buildIdTableCreateStatement( idTable, jdbcServices, metadata ),
				buildIdTableDropStatement( idTable, jdbcServices )
		);
	}

	@Override
	public void release(JdbcServices jdbcServices, JdbcConnectionAccess connectionAccess) {
		if ( serviceRegistry!=null && !dropTableStatements.isEmpty() ) {
			boolean dropIdTables = serviceRegistry.getService( ConfigurationService.class )
					.getSetting(
							GlobalTemporaryTableBulkIdStrategy.DROP_ID_TABLES,
							StandardConverters.BOOLEAN,
							false
					);
			if ( dropIdTables ) {
				ReactiveConnection connection =
						serviceRegistry.getService( ReactiveConnectionPool.class )
								.getProxyConnection();
				CompletionStages.loop( dropTableStatements, connection::execute )
						.whenComplete( (v, e) -> connection.close() )
						.handle( (v, e) -> null ) //ignore errors
						.toCompletableFuture().join();
			}
		}
	}

	@Override
	public UpdateHandler buildUpdateHandler(SessionFactoryImplementor factory, HqlSqlWalker walker) {
		return new TableBasedUpdateHandlerImpl( factory, walker, targetedPersister( walker ) );
	}

	@Override
	public DeleteHandler buildDeleteHandler(SessionFactoryImplementor factory, HqlSqlWalker walker) {
		return new TableBasedDeleteHandlerImpl( factory, walker, targetedPersister( walker ) );
	}

	private Queryable targetedPersister(HqlSqlWalker walker) {
		AbstractRestrictableStatement restrictableStatement = (AbstractRestrictableStatement) walker.getAST();
		FromElement fromElement = restrictableStatement.getFromClause().getFromElement();
		return fromElement.getQueryable();
	}

	private static class CreateTempTablesStatementsExecutor {
		private final ReactiveQueryExecutor session;
		private final String[] statements;
		private int currentIndex;

		private CreateTempTablesStatementsExecutor(ReactiveQueryExecutor session, String[] statements) {
			this.session = session;
			this.statements = statements;
		}

		public CompletionStage<Integer> execute(int i) {
			currentIndex = i;
			return session.getReactiveConnection()
					.executeStatement( statements[i] )
					.handle( this::ignoreException );
		}

		// ignore errors creating tables, since a create
		// table fails whenever the table already exists
		// or a drop table might fail if the table doesn't exist
		private Integer ignoreException(Void unused, Throwable throwable) {
			LOG.debugf( throwable, "Statement '%s' failed. Ignoring the exception", statements[currentIndex] );
			return 0;
		}
	}

	/**
	 * Handle the statements for the creation of temporary tables. They sometimes need to run
	 * as query instead of preparedQuery (it depends on the database).
	 */
	private abstract class CreateTempTablesHandler extends AbstractTableBasedBulkIdHandler
			implements StatementsWithParameters {

		private final String[] createTempTablesStatements;
		private final Queryable targetedPersister;

		public CreateTempTablesHandler(SessionFactoryImplementor sessionFactory, HqlSqlWalker walker, Queryable targetedPersister) {
			super( sessionFactory, walker );
			this.targetedPersister = targetedPersister;

			List<String> createTempStatements = new ArrayList<>();
			List<ParameterSpecification[]> createTempParameterSpecifications = new ArrayList<>();
			createTempTable( createTempStatements, createTempParameterSpecifications, getIdTableInfo( targetedPersister ) );

			this.createTempTablesStatements = ArrayHelper.toStringArray( createTempStatements );
		}

		@Override
		public Queryable getTargetedQueryable() {
			return targetedPersister;
		}

		@Override
		public CompletionStage<Integer> execute(ReactiveQueryExecutor session, QueryParameters queryParameters) {
			return executeCreateTempTablesStatements( session )
					.thenCompose( zero -> StatementsWithParameters.super.execute( session, queryParameters ) );
		}

		private CompletionStage<Integer> executeCreateTempTablesStatements(ReactiveQueryExecutor session) {
			CreateTempTablesStatementsExecutor createTempTables = new CreateTempTablesStatementsExecutor( session, createTempTablesStatements );
			return total( 0, createTempTablesStatements.length, createTempTables::execute );
		}

		@Override
		public boolean isTransactionalStatement(String statement) {
			return !db2 || !isSchemaDefinitionStatement( statement );
		}

		@Override
		protected String generateIdInsertSelect(String tableAlias, IdTableInfo idTableInfo, ProcessedWhereClause whereClause) {
			String sql = super.generateIdInsertSelect( tableAlias, idTableInfo, whereClause );
			return parameters.process( sql );
		}

		@Override
		protected String generateIdSubselect(Queryable persister, AbstractCollectionPersister cPersister, IdTableInfo idTableInfo) {
			String sql = super.generateIdSubselect( persister, cPersister, idTableInfo );
			return parameters.process( sql );
		}

		@Override
		protected String generateIdSubselect(Queryable persister, IdTableInfo idTableInfo) {
			String sql = super.generateIdSubselect( persister, idTableInfo );
			return parameters.process( sql );
		}

		@Override
		protected void prepareForUse(Queryable persister, SharedSessionContractImplementor session) {
			throw new UnsupportedOperationException();
		}

		@Override
		protected void releaseFromUse(Queryable persister, SharedSessionContractImplementor session) {
			throw new UnsupportedOperationException();
		}
	}

	private class TableBasedUpdateHandlerImpl extends CreateTempTablesHandler
			implements MultiTableBulkIdStrategy.UpdateHandler {

		private final String[] statements;
		private final ParameterSpecification[][] parameterSpecifications;

		@Override
		public ParameterSpecification[][] getParameterSpecifications() {
			return parameterSpecifications;
		}

		public TableBasedUpdateHandlerImpl(SessionFactoryImplementor factory, HqlSqlWalker walker, Queryable targetedPersister) {
			super( factory, walker,targetedPersister );

			UpdateStatement updateStatement = (UpdateStatement) walker.getAST();
			FromElement fromElement = updateStatement.getFromClause().getFromElement();
			String bulkTargetAlias = fromElement.getTableAlias();
			IdTableInfoImpl tableInfo = getIdTableInfo( targetedPersister );
			List<AssignmentSpecification> assignments = walker.getAssignmentSpecifications();

			String[] tableNames = targetedPersister.getConstraintOrderedTableNameClosure();
			String[][] columnNames = targetedPersister.getContraintOrderedTableKeyColumnClosure();

			ProcessedWhereClause processedWhereClause = processWhereClause( updateStatement.getWhereClause() );
			String idInsertSelect = generateIdInsertSelect( bulkTargetAlias, tableInfo, processedWhereClause );

			List<String> statements = new ArrayList<>();
			List<ParameterSpecification[]> parameterSpecifications = new ArrayList<>();
			statements.add( idInsertSelect );
//			if ( useSessionIdColumn() ) {
//				specifications.add( SESSION_ID );
//			}
			parameterSpecifications.add( processedWhereClause.getIdSelectParameterSpecifications().toArray( NO_PARAMS ) );

			String idSubselect = generateIdSubselect( targetedPersister, tableInfo );
			for ( int table=0; table<tableNames.length; table++ ) {
				String tableName = tableNames[table];
				List<AssignmentSpecification> tableAssignments =
						assignments.stream().filter( assignment -> assignment.affectsTable( tableName ) )
								.collect( Collectors.toList() );
				if ( !tableAssignments.isEmpty() ) {
					List<ParameterSpecification> parameterList = new ArrayList<>();
					String idColumnNames = String.join( ", ", columnNames[table] );
					Update update = new Update( walker.getDialect() ).setTableName( tableName );
					update.setWhere( "(" + idColumnNames + ") in (" + idSubselect + ")" );
					if ( factory().getSessionFactoryOptions().isCommentsEnabled() ) {
						update.setComment( "bulk update" );
					}
					for ( AssignmentSpecification assignment: tableAssignments ) {
						update.appendAssignmentFragment( assignment.getSqlAssignmentFragment() );
						if ( assignment.getParameters() != null ) {
							Collections.addAll( parameterList, assignment.getParameters() );
						}
					}
					String sql = parameters.process( update.toStatementString(), parameterList.size() );
					statements.add( sql );
//					if ( useSessionIdColumn() ) {
//						parameterList.add( SESSION_ID );
//					}
					parameterSpecifications.add( parameterList.toArray( NO_PARAMS ) );
				}
			}

			dropTempTable( statements, parameterSpecifications, tableInfo );

			this.statements = ArrayHelper.toStringArray( statements );
			this.parameterSpecifications = parameterSpecifications.toArray( new ParameterSpecification[0][] );
		}

		@Override
		public int execute(SharedSessionContractImplementor session, QueryParameters queryParameters) {
			throw new UnsupportedOperationException();
		}

		@Override
		public String[] getSqlStatements() {
			return statements;
		}

//		@Override
//		protected void addAnyExtraIdSelectValues(SelectValues selectClause) {
//			if ( usingTempTable && useSessionIdColumn() ) {
//				selectClause.addParameter( Types.CHAR, 36 );
//			}
//		}
//
//		@Override
//		protected String generateIdSubselect(Queryable persister, IdTableInfo idTableInfo) {
//			String sql = super.generateIdSubselect( persister, idTableInfo );
//			return useSessionIdColumn() ? sql + " where " + SESSION_ID_COLUMN_NAME + "=?" : sql;
//		}
	}

	private class TableBasedDeleteHandlerImpl extends CreateTempTablesHandler
			implements MultiTableBulkIdStrategy.DeleteHandler {

		private final String[] statements;
		private final ParameterSpecification[][] parameterSpecifications;

		@Override
		public ParameterSpecification[][] getParameterSpecifications() {
			return parameterSpecifications;
		}

		public TableBasedDeleteHandlerImpl(SessionFactoryImplementor factory, HqlSqlWalker walker, Queryable targetedPersister) {
			super( factory, walker, targetedPersister );

			List<String> statements = new ArrayList<>();
			List<ParameterSpecification[]> parameterSpecifications = new ArrayList<>();

			DeleteStatement deleteStatement = (DeleteStatement) walker.getAST();
			FromElement fromElement = deleteStatement.getFromClause().getFromElement();
			String bulkTargetAlias = fromElement.getTableAlias();
			IdTableInfoImpl tableInfo = getIdTableInfo( targetedPersister );

			ProcessedWhereClause processedWhereClause = processWhereClause( deleteStatement.getWhereClause() );
			String idInsertSelect = generateIdInsertSelect( bulkTargetAlias, tableInfo, processedWhereClause );
			statements.add( idInsertSelect );
//			if ( useSessionIdColumn() ) {
//				specifications.add( SESSION_ID );
//			}
			parameterSpecifications.add( processedWhereClause.getIdSelectParameterSpecifications().toArray( NO_PARAMS ) );

			// If many-to-many, delete the FK row in the collection table.
			// This partially overlaps with DeleteExecutor, but it instead
			// uses the temp table in the idSubselect.
			for ( Type type : targetedPersister.getPropertyTypes() ) {
				if ( type.isCollectionType() ) {
					CollectionType cType = (CollectionType) type;
					AbstractCollectionPersister cPersister = (AbstractCollectionPersister)
							factory.getMetamodel().collectionPersister( cType.getRole() );
					if ( cPersister.isManyToMany() ) {
						Delete delete = new Delete()
								.setTableName( cPersister.getTableName() )
								.setWhere( "(" + String.join( ", ", cPersister.getKeyColumnNames() )
										+ ") in (" + generateIdSubselect( targetedPersister, cPersister, tableInfo ) + ")" );
						if ( factory().getSessionFactoryOptions().isCommentsEnabled() ) {
							delete.setComment("bulk delete - m2m join table cleanup");
						}
						statements.add( delete.toStatementString() );
						parameterSpecifications.add( NO_PARAMS );
					}
				}
			}

			String idSubselect = generateIdSubselect( targetedPersister, tableInfo );
			String[] tableNames = targetedPersister.getConstraintOrderedTableNameClosure();
			String[][] columnNames = targetedPersister.getContraintOrderedTableKeyColumnClosure();
			for ( int table = 0; table < tableNames.length; table++ ) {
				String tableName = tableNames[table];
				String idColumnNames = String.join( ", ", columnNames[table] );
				Delete delete = new Delete().setTableName( tableName );
				delete.setWhere( "(" + idColumnNames + ") in (" + idSubselect + ")" );
				if ( factory().getSessionFactoryOptions().isCommentsEnabled() ) {
					delete.setComment("bulk delete");
				}
				statements.add( delete.toStatementString() );
				parameterSpecifications.add( NO_PARAMS );
//				parameterSpecifications.add( useSessionIdColumn() ? new ParameterSpecification[] { SESSION_ID } : NO_PARAMS );
			}

			dropTempTable( statements, parameterSpecifications, tableInfo );

			this.statements = ArrayHelper.toStringArray( statements );
			this.parameterSpecifications = parameterSpecifications.toArray( new ParameterSpecification[0][] );
		}

		@Override
		public String[] getSqlStatements() {
			return statements;
		}

		@Override
		public int execute(SharedSessionContractImplementor session, QueryParameters queryParameters) {
			throw new UnsupportedOperationException();
		}

//		@Override
//		protected void addAnyExtraIdSelectValues(SelectValues selectClause) {
//			if ( useSessionIdColumn() ) {
//				selectClause.addParameter( Types.VARCHAR, 36 );
//			}
//		}
//
//		@Override
//		protected String generateIdSubselect(Queryable persister, IdTableInfo idTableInfo) {
//			String sql = super.generateIdSubselect( persister, idTableInfo );
//			return useSessionIdColumn() ? sql + " where " + SESSION_ID_COLUMN_NAME + "=?" : sql;
//		}
//
//		@Override
//		protected String generateIdSubselect(Queryable persister, AbstractCollectionPersister collectionPersister,
//											 IdTableInfo idTableInfo) {
//			String sql = super.generateIdSubselect( persister, collectionPersister, idTableInfo );
//			return useSessionIdColumn() ? sql + " where " + SESSION_ID_COLUMN_NAME + "=?" : sql;
//		}
	}

	private void createTempTable(List<String> statements,
								 List<ParameterSpecification[]> parameterSpecifications,
								 IdTableInfoImpl tableInfo) {
		if (db2) {
			if ( createdGlobalTemporaryTables.add( tableInfo.getQualifiedIdTableName() ) ) {
				statements.add( tableInfo.getIdTableDropStatement() );
				parameterSpecifications.add( NO_PARAMS );
				statements.add( tableInfo.getIdTableCreationStatement() );
				parameterSpecifications.add( NO_PARAMS );
				dropTableStatements.add( tableInfo.getIdTableDropStatement() );
			}
		}
		else {
			statements.add( tableInfo.getIdTableCreationStatement() );
			parameterSpecifications.add( NO_PARAMS );
		}
	}

	private void dropTempTable(List<String> statements,
							   List<ParameterSpecification[]> parameterSpecifications,
							   IdTableInfoImpl tableInfo) {
//		if ( useSessionIdColumn() ) {
//			Delete drop = new Delete()
//					.setTableName( tableInfo.getQualifiedIdTableName() )
//					.setWhere( SESSION_ID_COLUMN_NAME + "=?" );
//			statements.add( drop.toStatementString() );
//			parameterSpecifications.add( new ParameterSpecification[] { SESSION_ID } );
//		}
//		else {
		if (db2) {
			statements.add( getIdTableSupport().getTruncateIdTableCommand() + " " + tableInfo.getQualifiedIdTableName() );
		}
		else {
			statements.add( tableInfo.getIdTableDropStatement() );
		}
		parameterSpecifications.add( NO_PARAMS );
	}

//	@Override
//	protected void augmentIdTableDefinition(Table idTable) {
//		if ( useSessionIdColumn() ) {
//			Column sessionIdColumn = new Column( SESSION_ID_COLUMN_NAME );
//			sessionIdColumn.setLength( 36 );
//			sessionIdColumn.setNullable( false );
//			SimpleValue value = new SimpleValue( metadata );
//			value.setTypeName( "string" );
//			sessionIdColumn.setValue( value );
//			sessionIdColumn.setComment( "Hibernate Session identifier" );
//			idTable.addColumn( sessionIdColumn );
//		}
//	}
}
