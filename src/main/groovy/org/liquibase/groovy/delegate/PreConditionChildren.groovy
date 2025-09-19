package org.liquibase.groovy.delegate
/* Generated @ Sat Sep 20 00:34:14 CEST 2025 on M600 */
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
		addPrecondition Tag.dbms, type
	}

	/** Check if the name of the database user executing the change log matches the {@code username}
	  @param username The name of the database user expected executing the change log */
	void runningAs( String username) {
		addPrecondition Tag.runningAs, username
	}

	/** Check if the changeset identified by {@code changeLogFile} :: {@code id} :: {@code author}
		has already been executed in a previous deployment
	  @param changeLogFile File name of the changelog. If not set the actual (logical) file name is used */
	void changeSetExecuted( String id, String author, String changeLogFile=null) {
		addPrecondition Tag.changeSetExecuted, id, author, changeLogFile
	}

	/** Check if the changeset identified by {@code changeLogFile} :: {@code id} :: {@code author}
		has already been executed in a previous deployment
	  @param changeLogFile File name of the changelog. If not set the actual (logical) file name is used */
	void changeSetExecuted( Map<String, Object> namedArgs, String id, String author, String changeLogFile=null) {
		addPrecondition Tag.changeSetExecuted, namedArgs, id, author, changeLogFile
	}

	/** Checks if table with {@code tableName} exists in the database */
	void tableExists( String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.tableExists, tableName, schemaName, catalogName
	}

	/** Checks if table with {@code tableName} exists in the database */
	void tableExists( Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.tableExists, namedArgs, tableName, schemaName, catalogName
	}

	/** Check if column {@code columnName} exists in the table {@code tableName} */
	void columnExists( String columnName, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.columnExists, columnName, tableName, schemaName, catalogName
	}

	/** Check if column {@code columnName} exists in the table {@code tableName} */
	void columnExists( Map<String, Object> namedArgs, String columnName, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.columnExists, namedArgs, columnName, tableName, schemaName, catalogName
	}

	/**  */
	void sequenceExists( String sequenceName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.sequenceExists, sequenceName, schemaName, catalogName
	}

	/**  */
	void sequenceExists( Map<String, Object> namedArgs, String sequenceName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.sequenceExists, namedArgs, sequenceName, schemaName, catalogName
	}

	/**  */
	void foreignKeyConstraintExists( String foreignKeyName, String foreignKeyTableName=null, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.foreignKeyConstraintExists, foreignKeyName, foreignKeyTableName, schemaName, catalogName
	}

	/**  */
	void foreignKeyConstraintExists( Map<String, Object> namedArgs, String foreignKeyName, String foreignKeyTableName=null, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.foreignKeyConstraintExists, namedArgs, foreignKeyName, foreignKeyTableName, schemaName, catalogName
	}

	/** Check if the index referenced by {@code indexName} or {@code tableName} and {@code columnNames}
		Either {@code indexName} or {@code tableName} and {@code columnNames} is required */
	void indexExists( String indexName=null, String tableName=null, String columnNames=null, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.indexExists, indexName, tableName, columnNames, schemaName, catalogName
	}

	/** Check if the index referenced by {@code indexName} or {@code tableName} and {@code columnNames}
		Either {@code indexName} or {@code tableName} and {@code columnNames} is required */
	void indexExists( Map<String, Object> namedArgs, String indexName=null, String tableName=null, String columnNames=null, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.indexExists, namedArgs, indexName, tableName, columnNames, schemaName, catalogName
	}

	/** Check if unique key exists on table defined by {@code tableName.} Either {@code primaryKeyName} or {@code tableName} required */
	void uniqueConstraintExists( String tableName, String constraintName=null, String columnNames=null, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.uniqueConstraintExists, tableName, constraintName, columnNames, schemaName, catalogName
	}

	/** Check if unique key exists on table defined by {@code tableName.} Either {@code primaryKeyName} or {@code tableName} required */
	void uniqueConstraintExists( Map<String, Object> namedArgs, String tableName, String constraintName=null, String columnNames=null, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.uniqueConstraintExists, namedArgs, tableName, constraintName, columnNames, schemaName, catalogName
	}

	/** Check if primary key exists. Either {@code primaryKeyName} or {@code tableName} required depending on the database type */
	void primaryKeyExists( String primaryKeyName=null, String tableName=null, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.primaryKeyExists, primaryKeyName, tableName, schemaName, catalogName
	}

	/** Check if primary key exists. Either {@code primaryKeyName} or {@code tableName} required depending on the database type */
	void primaryKeyExists( Map<String, Object> namedArgs, String primaryKeyName=null, String tableName=null, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.primaryKeyExists, namedArgs, primaryKeyName, tableName, schemaName, catalogName
	}

	/**  */
	void viewExists( String viewName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.viewExists, viewName, schemaName, catalogName
	}

	/**  */
	void viewExists( Map<String, Object> namedArgs, String viewName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.viewExists, namedArgs, viewName, schemaName, catalogName
	}

	/**  */
	void tableIsEmpty( String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.tableIsEmpty, tableName, schemaName, catalogName
	}

	/**  */
	void tableIsEmpty( Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.tableIsEmpty, namedArgs, tableName, schemaName, catalogName
	}

	/**  */
	void rowCount( Long expectedRows, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.rowCount, expectedRows, tableName, schemaName, catalogName
	}

	/**  */
	void rowCount( Map<String, Object> namedArgs, Long expectedRows, String tableName, String schemaName=null, String catalogName=null) {
		addPrecondition Tag.rowCount, namedArgs, expectedRows, tableName, schemaName, catalogName
	}

	/** Check if the changeLog property with the name in {@code property} is set to {@code value.}
		If {@code value} is not defined checks only if the property is defined at all */
	void changeLogPropertyDefined( String property, String value=null) {
		addPrecondition Tag.changeLogPropertyDefined, property, value
	}

	/** Precondition that checks if objectQuotingStrategy set for the changelog matches to {@code strategy}
	  @param strategy ObjectQuotingStrategy expected to be set for the changelog */
	void expectedQuotingStrategy( ObjectQuotingStrategy strategy) {
		addPrecondition Tag.expectedQuotingStrategy, strategy
	}

}
