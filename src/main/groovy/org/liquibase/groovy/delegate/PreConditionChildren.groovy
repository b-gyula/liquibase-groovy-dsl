package org.liquibase.groovy.delegate
/* Generated @ Wed Sep 24 20:40:43 CEST 2025 on M600 */
import groovy.transform.CompileStatic
import groovy.transform.SelfType
import liquibase.database.ObjectQuotingStrategy
import static org.liquibase.groovy.delegate.PreconditionDelegate.Tag

@CompileStatic
@SelfType(PreconditionDelegate)
trait PreConditionChildren {

	/** Check if the database the changelog executed on matches to {@code type}
	  @param type Expected database type */
	void dbms( String type) {
		addPrecondition args2Map(Tag.dbms) ('type',type)
	}

	/** Check if the name of the database user executing the change log matches the {@code username}
	  @param username The name of the database user expected executing the change log */
	void runningAs( String username) {
		addPrecondition args2Map(Tag.runningAs) ('username',username)
	}

	/** Check if the changeset identified by {@code changeLogFile} :: {@code id} :: {@code author}
		has already been executed in a previous deployment
	  @param changeLogFile File name of the changelog. If not set the actual (logical) file name is used */
	void changeSetExecuted( String id, String author, String changeLogFile=null) {
		addPrecondition args2Map(Tag.changeSetExecuted) ('id',id) ('author',author) ('changeLogFile',changeLogFile)
	}

	/** Check if the changeset identified by {@code changeLogFile} :: {@code id} :: {@code author}
		has already been executed in a previous deployment
	  @param changeLogFile File name of the changelog. If not set the actual (logical) file name is used */
	void changeSetExecuted( Map<String, Object> namedArgs, String id, String author, String changeLogFile=null) {
		addPrecondition args2Map(Tag.changeSetExecuted,namedArgs) ('id',id) ('author',author) ('changeLogFile',changeLogFile)
	}

	/** Checks if table with {@code tableName} exists in the database */
	void tableExists( String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.tableExists) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/** Checks if table with {@code tableName} exists in the database */
	void tableExists( Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.tableExists,namedArgs) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/** Check if column {@code columnName} exists in the table {@code tableName} */
	void columnExists( String columnName, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.columnExists) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/** Check if column {@code columnName} exists in the table {@code tableName} */
	void columnExists( Map<String, Object> namedArgs, String columnName, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.columnExists,namedArgs) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/**  */
	void sequenceExists( String sequenceName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.sequenceExists) ('sequenceName',sequenceName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/**  */
	void sequenceExists( Map<String, Object> namedArgs, String sequenceName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.sequenceExists,namedArgs) ('sequenceName',sequenceName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/**  */
	void foreignKeyConstraintExists( String foreignKeyName, String foreignKeyTableName=null, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.foreignKeyConstraintExists) ('foreignKeyName',foreignKeyName) ('foreignKeyTableName',foreignKeyTableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/**  */
	void foreignKeyConstraintExists( Map<String, Object> namedArgs, String foreignKeyName, String foreignKeyTableName=null, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.foreignKeyConstraintExists,namedArgs) ('foreignKeyName',foreignKeyName) ('foreignKeyTableName',foreignKeyTableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/** Check if the index referenced by {@code indexName} or {@code tableName} and {@code columnNames}
		Either {@code indexName} or {@code tableName} and {@code columnNames} is required */
	void indexExists( String indexName=null, String tableName=null, String columnNames=null, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.indexExists) ('indexName',indexName) ('tableName',tableName) ('columnNames',columnNames) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/** Check if the index referenced by {@code indexName} or {@code tableName} and {@code columnNames}
		Either {@code indexName} or {@code tableName} and {@code columnNames} is required */
	void indexExists( Map<String, Object> namedArgs, String indexName=null, String tableName=null, String columnNames=null, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.indexExists,namedArgs) ('indexName',indexName) ('tableName',tableName) ('columnNames',columnNames) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/** Check if unique key exists on table defined by {@code tableName.} Either {@code primaryKeyName} or {@code tableName} required */
	void uniqueConstraintExists( String tableName, String constraintName=null, String columnNames=null, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.uniqueConstraintExists) ('tableName',tableName) ('constraintName',constraintName) ('columnNames',columnNames) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/** Check if unique key exists on table defined by {@code tableName.} Either {@code primaryKeyName} or {@code tableName} required */
	void uniqueConstraintExists( Map<String, Object> namedArgs, String tableName, String constraintName=null, String columnNames=null, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.uniqueConstraintExists,namedArgs) ('tableName',tableName) ('constraintName',constraintName) ('columnNames',columnNames) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/** Check if primary key exists. Either {@code primaryKeyName} or {@code tableName} required depending on the database type */
	void primaryKeyExists( String primaryKeyName=null, String tableName=null, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.primaryKeyExists) ('primaryKeyName',primaryKeyName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/** Check if primary key exists. Either {@code primaryKeyName} or {@code tableName} required depending on the database type */
	void primaryKeyExists( Map<String, Object> namedArgs, String primaryKeyName=null, String tableName=null, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.primaryKeyExists,namedArgs) ('primaryKeyName',primaryKeyName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/**  */
	void viewExists( String viewName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.viewExists) ('viewName',viewName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/**  */
	void viewExists( Map<String, Object> namedArgs, String viewName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.viewExists,namedArgs) ('viewName',viewName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/**  */
	void tableIsEmpty( String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.tableIsEmpty) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/**  */
	void tableIsEmpty( Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.tableIsEmpty,namedArgs) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/**  */
	void rowCount( Long expectedRows, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.rowCount) ('expectedRows',expectedRows) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/**  */
	void rowCount( Map<String, Object> namedArgs, Long expectedRows, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition args2Map(Tag.rowCount,namedArgs) ('expectedRows',expectedRows) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName)
	}

	/** Check if the changeLog property with the name in {@code property} is set to {@code value.}
		If {@code value} is not defined checks only if the property is defined at all */
	void changeLogPropertyDefined( String property, String value=null) {
		addPrecondition args2Map(Tag.changeLogPropertyDefined) ('property',property) ('value',value)
	}

	/** Precondition that checks if objectQuotingStrategy set for the changelog matches to {@code strategy}
	  @param strategy ObjectQuotingStrategy expected to be set for the changelog */
	void expectedQuotingStrategy( ObjectQuotingStrategy strategy) {
		addPrecondition args2Map(Tag.expectedQuotingStrategy) ('strategy',strategy)
	}

}
