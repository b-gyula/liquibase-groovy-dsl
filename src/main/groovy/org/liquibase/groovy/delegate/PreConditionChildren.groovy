package org.liquibase.groovy.delegate
/* Generated @ Fri Oct 25 18:53:18 CEST 2024 */
import groovy.transform.CompileStatic
import groovy.transform.SelfType
import liquibase.database.ObjectQuotingStrategy
import static org.liquibase.groovy.delegate.PreconditionDelegate.Tag.*

@CompileStatic
@SelfType(PreconditionDelegate)
trait PreConditionChildren {

    
	/** Check if the database the changelog executed on matches to @type 
	  @param type Expected database type
	 */
    void dbms(String type) {
        addPrecondition dbms, [:],  type
    }

    
	/** Check if the name of the database user executing the change log matches the @username 
	  @param username The name of the database user expected executing the change log
	 */
    void runningAs(String username) {
        addPrecondition runningAs, [:],  username
    }

    
	/** Check if the changeset identified by @changeLogFile :: @id :: @author
		has already been executed in a previous deployment 
	  @param changeLogFile File name of the changelog. If not set the actual (logical) file name is used
	 */
    void changeSetExecuted(String id, String author, String changeLogFile=null) {
        addPrecondition changeSetExecuted, [:],  id,  author,  changeLogFile
    }

    
	/** Check if the changeset identified by @changeLogFile :: @id :: @author
		has already been executed in a previous deployment 
	  @param changeLogFile File name of the changelog. If not set the actual (logical) file name is used
	 */
    void changeSetExecuted(Map<String, Object> namedArgs, String id, String author, String changeLogFile=null) {
        addPrecondition changeSetExecuted, namedArgs,  id,  author,  changeLogFile
    }

    
	/** Checks if table with @tableName exists in the database  */
    void tableExists(String tableName, String schemaName=null, String catalogName=null) {
        addPrecondition tableExists, [:],  tableName,  schemaName,  catalogName
    }

    
	/** Checks if table with @tableName exists in the database  */
    void tableExists(Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null) {
        addPrecondition tableExists, namedArgs,  tableName,  schemaName,  catalogName
    }

    
	/** Check if column @columnName exists in the table @tableName  */
    void columnExists(String columnName, String tableName, String schemaName=null, String catalogName=null) {
        addPrecondition columnExists, [:],  columnName,  tableName,  schemaName,  catalogName
    }

    
	/** Check if column @columnName exists in the table @tableName  */
    void columnExists(Map<String, Object> namedArgs, String columnName, String tableName, String schemaName=null, String catalogName=null) {
        addPrecondition columnExists, namedArgs,  columnName,  tableName,  schemaName,  catalogName
    }

    
	/**   */
    void sequenceExists(String sequenceName, String schemaName=null, String catalogName=null) {
        addPrecondition sequenceExists, [:],  sequenceName,  schemaName,  catalogName
    }

    
	/**   */
    void sequenceExists(Map<String, Object> namedArgs, String sequenceName, String schemaName=null, String catalogName=null) {
        addPrecondition sequenceExists, namedArgs,  sequenceName,  schemaName,  catalogName
    }

    
	/**   */
    void foreignKeyConstraintExists(String foreignKeyName, String foreignKeyTableName=null, String schemaName=null, String catalogName=null) {
        addPrecondition foreignKeyConstraintExists, [:],  foreignKeyName,  foreignKeyTableName,  schemaName,  catalogName
    }

    
	/**   */
    void foreignKeyConstraintExists(Map<String, Object> namedArgs, String foreignKeyName, String foreignKeyTableName=null, String schemaName=null, String catalogName=null) {
        addPrecondition foreignKeyConstraintExists, namedArgs,  foreignKeyName,  foreignKeyTableName,  schemaName,  catalogName
    }

    
	/** Check if the index referenced by @indexName or @tableName and @columnNames
		Either @indexName or @tableName and @columnNames is required  */
    void indexExists(String indexName=null, String tableName=null, String columnNames=null, String schemaName=null, String catalogName=null) {
        addPrecondition indexExists, [:],  indexName,  tableName,  columnNames,  schemaName,  catalogName
    }


	/** Check if the index referenced by @indexName or @tableName and @columnNames
		Either @indexName or @tableName and @columnNames is required  */
    void indexExists(Map<String, Object> namedArgs, String indexName=null, String tableName=null, String columnNames=null, String schemaName=null, String catalogName=null) {
        addPrecondition indexExists, namedArgs,  indexName,  tableName,  columnNames,  schemaName,  catalogName
    }


	/** Check if unique key exists on table defined by @tableName. Either @primaryKeyName or @tableName required  */
    void uniqueConstraintExists(String constraintName=null, String tableName, String columnNames=null, String schemaName=null, String catalogName=null) {
        addPrecondition uniqueConstraintExists, [:],  constraintName,  tableName,  columnNames,  schemaName,  catalogName
    }

    
	/** Check if unique key exists on table defined by @tableName. Either @primaryKeyName or @tableName required  */
    void uniqueConstraintExists(Map<String, Object> namedArgs, String constraintName=null, String tableName, String columnNames=null, String schemaName=null, String catalogName=null) {
        addPrecondition uniqueConstraintExists, namedArgs,  constraintName,  tableName,  columnNames,  schemaName,  catalogName
    }

    
	/** Check if primary key exists. Either @primaryKeyName or @tableName required depending on the database type  */
    void primaryKeyExists(String primaryKeyName=null, String tableName=null, String schemaName=null, String catalogName=null) {
        addPrecondition primaryKeyExists, [:],  primaryKeyName,  tableName,  schemaName,  catalogName
    }

    
	/** Check if primary key exists. Either @primaryKeyName or @tableName required depending on the database type  */
    void primaryKeyExists(Map<String, Object> namedArgs, String primaryKeyName=null, String tableName=null, String schemaName=null, String catalogName=null) {
        addPrecondition primaryKeyExists, namedArgs,  primaryKeyName,  tableName,  schemaName,  catalogName
    }

    
	/**   */
    void viewExists(String viewName, String schemaName=null, String catalogName=null) {
        addPrecondition viewExists, [:],  viewName,  schemaName,  catalogName
    }

    
	/**   */
    void viewExists(Map<String, Object> namedArgs, String viewName, String schemaName=null, String catalogName=null) {
        addPrecondition viewExists, namedArgs,  viewName,  schemaName,  catalogName
    }

    
	/**   */
    void tableIsEmpty(String tableName, String schemaName=null, String catalogName=null) {
        addPrecondition tableIsEmpty, [:],  tableName,  schemaName,  catalogName
    }

    
	/**   */
    void tableIsEmpty(Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null) {
        addPrecondition tableIsEmpty, namedArgs,  tableName,  schemaName,  catalogName
    }

    
	/**   */
    void rowCount(Long expectedRows, String tableName, String schemaName=null, String catalogName=null) {
        addPrecondition rowCount, [:],  expectedRows,  tableName,  schemaName,  catalogName
    }

    
	/**   */
    void rowCount(Map<String, Object> namedArgs, Long expectedRows, String tableName, String schemaName=null, String catalogName=null) {
        addPrecondition rowCount, namedArgs,  expectedRows,  tableName,  schemaName,  catalogName
    }

    
	/** Check if the changeLog property with the name in @property is set to @value.
		If @value is not defined checks only if the property is defined at all  */
    void changeLogPropertyDefined(String property, String value=null) {
        addPrecondition changeLogPropertyDefined, [:],  property,  value
    }


	/** Precondition that checks if objectQuotingStrategy set for the changelog matches to @strategy 
	  @param strategy ObjectQuotingStrategy expected to be set for the changelog
	 */
    void expectedQuotingStrategy(ObjectQuotingStrategy strategy) {
        addPrecondition expectedQuotingStrategy, [:],  strategy
    }

}
