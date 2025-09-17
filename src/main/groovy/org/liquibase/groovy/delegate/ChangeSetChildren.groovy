package org.liquibase.groovy.delegate
/* Generated @ Thu Jan 30 11:25:37 CET 2025 on M600 */
import groovy.transform.CompileStatic
import groovy.transform.SelfType
import liquibase.database.ColumnParentTypeEnum
import liquibase.database.FkCascadeActionOptions
import static org.liquibase.groovy.delegate.ChangeSetDelegate.*
import static groovy.lang.Closure.DELEGATE_ONLY

@CompileStatic
@SelfType(ChangeSetDelegate)
trait ChangeSetChildren {

	/** Create a table with the defined columns  */
	void createTable( String tableName, Boolean ifNotExists=null, String schemaName=null, String catalogName=null, String tablespace=null, String tableType=null, String remarks=null, Boolean rowDependencies=null, 
				@DelegatesTo(value=CreateTableDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.createTable, tableName, ifNotExists, schemaName, catalogName, tablespace, tableType, remarks, rowDependencies, columns
	}

	/** Create a table with the defined columns  */
	void createTable(Map<String, Object> namedArgs, String tableName, Boolean ifNotExists=null, String schemaName=null, String catalogName=null, String tablespace=null, String tableType=null, String remarks=null, Boolean rowDependencies=null, 
				@DelegatesTo(value=CreateTableDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.createTable, namedArgs, tableName, ifNotExists, schemaName, catalogName, tablespace, tableType, remarks, rowDependencies, columns
	}

	/** Create a table with the defined columns  */
	
	void createTable(Map<String, Object> params, 
				@DelegatesTo(value=CreateTableDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChangeWithChild Tag.createTable, params, columns
	}
	
	/**   */
	void dropTable( String tableName, Boolean cascadeConstraints=null, String schemaName=null, String catalogName=null) {
		addChange Tag.dropTable, tableName, cascadeConstraints, schemaName, catalogName
	}

	/**   */
	void dropTable(Map<String, Object> namedArgs, String tableName, Boolean cascadeConstraints=null, String schemaName=null, String catalogName=null) {
		addChange Tag.dropTable, namedArgs, tableName, cascadeConstraints, schemaName, catalogName
	}

	/**   */
	
	void dropTable(Map<String, Object> params) {
		addMapBasedChange Tag.dropTable, params
	}

	/**  
	 <br>Params:<dl>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the path defined in {@code path} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	</dl> */
	void createView( String viewName, Boolean replaceIfExists=null, Boolean fullDefinition=null, String path=null, Boolean relativeToChangelogFile=null, String remarks=null, String encoding=null, String schemaName=null, String catalogName=null) {
		addChange Tag.createView, viewName, replaceIfExists, fullDefinition, path, relativeToChangelogFile, remarks, encoding, schemaName, catalogName
	}

	/**  
	 <br>Params:<dl>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the path defined in {@code path} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	</dl> */
	void createView(Map<String, Object> namedArgs, String viewName, Boolean replaceIfExists=null, Boolean fullDefinition=null, String path=null, Boolean relativeToChangelogFile=null, String remarks=null, String encoding=null, String schemaName=null, String catalogName=null) {
		addChange Tag.createView, namedArgs, viewName, replaceIfExists, fullDefinition, path, relativeToChangelogFile, remarks, encoding, schemaName, catalogName
	}

	/**   */
	void renameView( String oldViewName, String newViewName, String schemaName=null, String catalogName=null) {
		addChange Tag.renameView, oldViewName, newViewName, schemaName, catalogName
	}

	/**   */
	void renameView(Map<String, Object> namedArgs, String oldViewName, String newViewName, String schemaName=null, String catalogName=null) {
		addChange Tag.renameView, namedArgs, oldViewName, newViewName, schemaName, catalogName
	}

	/**   */
	
	void renameView(Map<String, Object> params) {
		addMapBasedChange Tag.renameView, params
	}

	/**   */
	void dropView( String viewName, Boolean ifExists=null, String schemaName=null, String catalogName=null) {
		addChange Tag.dropView, viewName, ifExists, schemaName, catalogName
	}

	/**   */
	void dropView(Map<String, Object> namedArgs, String viewName, Boolean ifExists=null, String schemaName=null, String catalogName=null) {
		addChange Tag.dropView, namedArgs, viewName, ifExists, schemaName, catalogName
	}

	/**   */
	
	void dropView(Map<String, Object> params) {
		addMapBasedChange Tag.dropView, params
	}

	/** Inserts data into an existing table 
	 <br>Params:<dl>
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	</dl> */
	void insert( String tableName, String schemaName=null, String catalogName=null, String dbms=null, 
				@DelegatesTo(value=DataColumn, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.insert, tableName, schemaName, catalogName, dbms, columns
	}

	/** Inserts data into an existing table 
	 <br>Params:<dl>
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	</dl> */
	void insert(Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null, String dbms=null, 
				@DelegatesTo(value=DataColumn, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.insert, namedArgs, tableName, schemaName, catalogName, dbms, columns
	}

	/** Inserts data into an existing table 
	 <br>Params:<dl>
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	</dl> */
	
	void insert(Map<String, Object> params, 
				@DelegatesTo(value=DataColumn, strategy=DELEGATE_ONLY) Closure columns) {
		addChangeWithChild Tag.insert, params, columns
	}
	
	/**   */
	void addColumn( String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=AddColumnDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.addColumn, tableName, schemaName, catalogName, columns
	}

	/**   */
	void addColumn(Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=AddColumnDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.addColumn, namedArgs, tableName, schemaName, catalogName, columns
	}

	/**   */
	
	void addColumn(Map<String, Object> params, 
				@DelegatesTo(value=AddColumnDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChangeWithChild Tag.addColumn, params, columns
	}
	
	/**   */
	void dropProcedure( String procedureName, String schemaName=null, String catalogName=null) {
		addChange Tag.dropProcedure, procedureName, schemaName, catalogName
	}

	/**   */
	void dropProcedure(Map<String, Object> namedArgs, String procedureName, String schemaName=null, String catalogName=null) {
		addChange Tag.dropProcedure, namedArgs, procedureName, schemaName, catalogName
	}

	/**   */
	
	void dropProcedure(Map<String, Object> params) {
		addMapBasedChange Tag.dropProcedure, params
	}

	/** Execute any SQL statement(s) in the external file defined in {@code path.}
		The SQL can also contain comments of either of the following formats:
		A multi-line comment that starts with /* and ends with *\/.
		A single line comment starting with -- and finishing at the end of the line.
		or a comment element can be used 
	 <br>Params:<dl>
	 <dt><b>path</b></dt>
		<dd>Name of the file containing the SQL statements to execute</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the path defined in {@code path} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>stripComments</dt>
		<dd>When true, any comments are removed in the statement before executing on the database. Default: true.</dd>
	 <dt>splitStatements</dt>
		<dd>When true, Liquibase splits statements on {@code endDelimiter} and executes them separately. Default: true.</dd>
	 <dt>endDelimiter</dt>
		<dd>The delimiter to separate raw SQL statements. The default value is `;`
		See: https://docs.liquibase.com/change-types/enddelimiter-sql.html</dd>
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	</dl> */
	void sqlFile( String path, Boolean relativeToChangelogFile=null, Boolean stripComments=null, Boolean splitStatements=null, String endDelimiter=null, String dbms=null, String encoding=null) {
		addChange Tag.sqlFile, path, relativeToChangelogFile, stripComments, splitStatements, endDelimiter, dbms, encoding
	}

	/** Execute any SQL statement(s) in the external file defined in {@code path.}
		The SQL can also contain comments of either of the following formats:
		A multi-line comment that starts with /* and ends with *\/.
		A single line comment starting with -- and finishing at the end of the line.
		or a comment element can be used 
	 <br>Params:<dl>
	 <dt><b>path</b></dt>
		<dd>Name of the file containing the SQL statements to execute</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the path defined in {@code path} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>stripComments</dt>
		<dd>When true, any comments are removed in the statement before executing on the database. Default: true.</dd>
	 <dt>splitStatements</dt>
		<dd>When true, Liquibase splits statements on {@code endDelimiter} and executes them separately. Default: true.</dd>
	 <dt>endDelimiter</dt>
		<dd>The delimiter to separate raw SQL statements. The default value is `;`
		See: https://docs.liquibase.com/change-types/enddelimiter-sql.html</dd>
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	</dl> */
	void sqlFile(Map<String, Object> namedArgs, String path, Boolean relativeToChangelogFile=null, Boolean stripComments=null, Boolean splitStatements=null, String endDelimiter=null, String dbms=null, String encoding=null) {
		addChange Tag.sqlFile, namedArgs, path, relativeToChangelogFile, stripComments, splitStatements, endDelimiter, dbms, encoding
	}

	/** Execute any SQL statement(s) in the external file defined in {@code path.}
		The SQL can also contain comments of either of the following formats:
		A multi-line comment that starts with /* and ends with *\/.
		A single line comment starting with -- and finishing at the end of the line.
		or a comment element can be used 
	 <br>Params:<dl>
	 <dt><b>path</b></dt>
		<dd>Name of the file containing the SQL statements to execute</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the path defined in {@code path} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>stripComments</dt>
		<dd>When true, any comments are removed in the statement before executing on the database. Default: true.</dd>
	 <dt>splitStatements</dt>
		<dd>When true, Liquibase splits statements on {@code endDelimiter} and executes them separately. Default: true.</dd>
	 <dt>endDelimiter</dt>
		<dd>The delimiter to separate raw SQL statements. The default value is `;`
		See: https://docs.liquibase.com/change-types/enddelimiter-sql.html</dd>
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	</dl> */
	
	void sqlFile(Map<String, Object> params) {
		addMapBasedChange Tag.sqlFile, params
	}

	/**   */
	void renameTable( String oldTableName, String newTableName, String schemaName=null, String catalogName=null) {
		addChange Tag.renameTable, oldTableName, newTableName, schemaName, catalogName
	}

	/**   */
	void renameTable(Map<String, Object> namedArgs, String oldTableName, String newTableName, String schemaName=null, String catalogName=null) {
		addChange Tag.renameTable, namedArgs, oldTableName, newTableName, schemaName, catalogName
	}

	/**   */
	
	void renameTable(Map<String, Object> params) {
		addMapBasedChange Tag.renameTable, params
	}

	/**   */
	void renameColumn( String oldColumnName, String newColumnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null, String remarks=null) {
		addChange Tag.renameColumn, oldColumnName, newColumnName, tableName, schemaName, catalogName, columnDataType, remarks
	}

	/**   */
	void renameColumn(Map<String, Object> namedArgs, String oldColumnName, String newColumnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null, String remarks=null) {
		addChange Tag.renameColumn, namedArgs, oldColumnName, newColumnName, tableName, schemaName, catalogName, columnDataType, remarks
	}

	/**   */
	
	void renameColumn(Map<String, Object> params) {
		addMapBasedChange Tag.renameColumn, params
	}

	/**   */
	void mergeColumns( String column1Name, String joinString, String column2Name, String finalColumnName, String finalColumnType, String tableName, String schemaName=null, String catalogName=null) {
		addChange Tag.mergeColumns, column1Name, joinString, column2Name, finalColumnName, finalColumnType, tableName, schemaName, catalogName
	}

	/**   */
	void mergeColumns(Map<String, Object> namedArgs, String column1Name, String joinString, String column2Name, String finalColumnName, String finalColumnType, String tableName, String schemaName=null, String catalogName=null) {
		addChange Tag.mergeColumns, namedArgs, column1Name, joinString, column2Name, finalColumnName, finalColumnType, tableName, schemaName, catalogName
	}

	/**   */
	
	void mergeColumns(Map<String, Object> params) {
		addMapBasedChange Tag.mergeColumns, params
	}

	/**   */
	void modifyDataType( String newDataType, String columnName, String tableName, String schemaName=null, String catalogName=null) {
		addChange Tag.modifyDataType, newDataType, columnName, tableName, schemaName, catalogName
	}

	/**   */
	void modifyDataType(Map<String, Object> namedArgs, String newDataType, String columnName, String tableName, String schemaName=null, String catalogName=null) {
		addChange Tag.modifyDataType, namedArgs, newDataType, columnName, tableName, schemaName, catalogName
	}

	/**   */
	
	void modifyDataType(Map<String, Object> params) {
		addMapBasedChange Tag.modifyDataType, params
	}

	/**  
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	void createSequence( String sequenceName, Integer startValue=null, Integer incrementBy=null, Integer minValue=null, Integer maxValue=null, Boolean ordered=null, String cacheSize=null, String dataType=null, Boolean cycle=null, String schemaName=null, String catalogName=null) {
		addChange Tag.createSequence, sequenceName, startValue, incrementBy, minValue, maxValue, ordered, cacheSize, dataType, cycle, schemaName, catalogName
	}

	/**  
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	void createSequence(Map<String, Object> namedArgs, String sequenceName, Integer startValue=null, Integer incrementBy=null, Integer minValue=null, Integer maxValue=null, Boolean ordered=null, String cacheSize=null, String dataType=null, Boolean cycle=null, String schemaName=null, String catalogName=null) {
		addChange Tag.createSequence, namedArgs, sequenceName, startValue, incrementBy, minValue, maxValue, ordered, cacheSize, dataType, cycle, schemaName, catalogName
	}

	/**  
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	
	void createSequence(Map<String, Object> params) {
		addMapBasedChange Tag.createSequence, params
	}

	/**  
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	void alterSequence( String sequenceName, Integer incrementBy=null, Integer minValue=null, Integer maxValue=null, Boolean ordered=null, String cacheSize=null, String dataType=null, Boolean cycle=null, String schemaName=null, String catalogName=null) {
		addChange Tag.alterSequence, sequenceName, incrementBy, minValue, maxValue, ordered, cacheSize, dataType, cycle, schemaName, catalogName
	}

	/**  
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	void alterSequence(Map<String, Object> namedArgs, String sequenceName, Integer incrementBy=null, Integer minValue=null, Integer maxValue=null, Boolean ordered=null, String cacheSize=null, String dataType=null, Boolean cycle=null, String schemaName=null, String catalogName=null) {
		addChange Tag.alterSequence, namedArgs, sequenceName, incrementBy, minValue, maxValue, ordered, cacheSize, dataType, cycle, schemaName, catalogName
	}

	/**  
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	
	void alterSequence(Map<String, Object> params) {
		addMapBasedChange Tag.alterSequence, params
	}

	/**   */
	void dropSequence( String sequenceName, String schemaName=null, String catalogName=null) {
		addChange Tag.dropSequence, sequenceName, schemaName, catalogName
	}

	/**   */
	void dropSequence(Map<String, Object> namedArgs, String sequenceName, String schemaName=null, String catalogName=null) {
		addChange Tag.dropSequence, namedArgs, sequenceName, schemaName, catalogName
	}

	/**   */
	
	void dropSequence(Map<String, Object> params) {
		addMapBasedChange Tag.dropSequence, params
	}

	/**   */
	void renameSequence( String oldSequenceName, String newSequenceName, String schemaName=null, String catalogName=null) {
		addChange Tag.renameSequence, oldSequenceName, newSequenceName, schemaName, catalogName
	}

	/**   */
	void renameSequence(Map<String, Object> namedArgs, String oldSequenceName, String newSequenceName, String schemaName=null, String catalogName=null) {
		addChange Tag.renameSequence, namedArgs, oldSequenceName, newSequenceName, schemaName, catalogName
	}

	/**   */
	
	void renameSequence(Map<String, Object> params) {
		addMapBasedChange Tag.renameSequence, params
	}

	/**   */
	void createIndex( String indexName, String tableName, String schemaName=null, String catalogName=null, String associatedWith=null, Boolean unique=null, Boolean clustered=null, String tablespace=null, 
				@DelegatesTo(value=CreateIndexDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.createIndex, indexName, tableName, schemaName, catalogName, associatedWith, unique, clustered, tablespace, columns
	}

	/**   */
	void createIndex(Map<String, Object> namedArgs, String indexName, String tableName, String schemaName=null, String catalogName=null, String associatedWith=null, Boolean unique=null, Boolean clustered=null, String tablespace=null, 
				@DelegatesTo(value=CreateIndexDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.createIndex, namedArgs, indexName, tableName, schemaName, catalogName, associatedWith, unique, clustered, tablespace, columns
	}

	/**   */
	
	void createIndex(Map<String, Object> params, 
				@DelegatesTo(value=CreateIndexDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChangeWithChild Tag.createIndex, params, columns
	}
	
	/**   */
	void dropIndex( String indexName, String tableName, String schemaName=null, String catalogName=null, String associatedWith=null) {
		addChange Tag.dropIndex, indexName, tableName, schemaName, catalogName, associatedWith
	}

	/**   */
	void dropIndex(Map<String, Object> namedArgs, String indexName, String tableName, String schemaName=null, String catalogName=null, String associatedWith=null) {
		addChange Tag.dropIndex, namedArgs, indexName, tableName, schemaName, catalogName, associatedWith
	}

	/**   */
	
	void dropIndex(Map<String, Object> params) {
		addMapBasedChange Tag.dropIndex, params
	}

	/**   */
	void addNotNullConstraint( String columnName, String tableName, String schemaName=null, String catalogName=null, String defaultNullValue=null, String columnDataType=null, String constraintName=null, Boolean validate=null) {
		addChange Tag.addNotNullConstraint, columnName, tableName, schemaName, catalogName, defaultNullValue, columnDataType, constraintName, validate
	}

	/**   */
	void addNotNullConstraint(Map<String, Object> namedArgs, String columnName, String tableName, String schemaName=null, String catalogName=null, String defaultNullValue=null, String columnDataType=null, String constraintName=null, Boolean validate=null) {
		addChange Tag.addNotNullConstraint, namedArgs, columnName, tableName, schemaName, catalogName, defaultNullValue, columnDataType, constraintName, validate
	}

	/**   */
	
	void addNotNullConstraint(Map<String, Object> params) {
		addMapBasedChange Tag.addNotNullConstraint, params
	}

	/**   */
	void dropNotNullConstraint( String constraintName=null, String tableName, String schemaName=null, String catalogName=null, String columnName=null, String columnDataType=null) {
		addChange Tag.dropNotNullConstraint, constraintName, tableName, schemaName, catalogName, columnName, columnDataType
	}

	/**   */
	void dropNotNullConstraint(Map<String, Object> namedArgs, String constraintName=null, String tableName, String schemaName=null, String catalogName=null, String columnName=null, String columnDataType=null) {
		addChange Tag.dropNotNullConstraint, namedArgs, constraintName, tableName, schemaName, catalogName, columnName, columnDataType
	}

	/**   */
	
	void dropNotNullConstraint(Map<String, Object> params) {
		addMapBasedChange Tag.dropNotNullConstraint, params
	}

	/**   */
	void addForeignKeyConstraint( String baseTableName, String baseTableSchemaName=null, String baseTableCatalogName=null, String baseColumnNames, String constraintName, String referencedTableName, String referencedColumnNames, String referencedTableSchemaName=null, String referencedTableCatalogName=null, Boolean deferrable=null, Boolean initiallyDeferred=null, Boolean deleteCascade=null, FkCascadeActionOptions onDelete=null, FkCascadeActionOptions onUpdate=null, Boolean referencesUniqueColumn=null, Boolean validate=null) {
		addChange Tag.addForeignKeyConstraint, baseTableName, baseTableSchemaName, baseTableCatalogName, baseColumnNames, constraintName, referencedTableName, referencedColumnNames, referencedTableSchemaName, referencedTableCatalogName, deferrable, initiallyDeferred, deleteCascade, onDelete, onUpdate, referencesUniqueColumn, validate
	}

	/**   */
	void addForeignKeyConstraint(Map<String, Object> namedArgs, String baseTableName, String baseTableSchemaName=null, String baseTableCatalogName=null, String baseColumnNames, String constraintName, String referencedTableName, String referencedColumnNames, String referencedTableSchemaName=null, String referencedTableCatalogName=null, Boolean deferrable=null, Boolean initiallyDeferred=null, Boolean deleteCascade=null, FkCascadeActionOptions onDelete=null, FkCascadeActionOptions onUpdate=null, Boolean referencesUniqueColumn=null, Boolean validate=null) {
		addChange Tag.addForeignKeyConstraint, namedArgs, baseTableName, baseTableSchemaName, baseTableCatalogName, baseColumnNames, constraintName, referencedTableName, referencedColumnNames, referencedTableSchemaName, referencedTableCatalogName, deferrable, initiallyDeferred, deleteCascade, onDelete, onUpdate, referencesUniqueColumn, validate
	}

	/**   */
	
	void addForeignKeyConstraint(Map<String, Object> params) {
		addMapBasedChange Tag.addForeignKeyConstraint, params
	}

	/**   */
	void dropForeignKeyConstraint( String constraintName, String baseTableName, String baseTableSchemaName=null, String baseTableCatalogName=null) {
		addChange Tag.dropForeignKeyConstraint, constraintName, baseTableName, baseTableSchemaName, baseTableCatalogName
	}

	/**   */
	void dropForeignKeyConstraint(Map<String, Object> namedArgs, String constraintName, String baseTableName, String baseTableSchemaName=null, String baseTableCatalogName=null) {
		addChange Tag.dropForeignKeyConstraint, namedArgs, constraintName, baseTableName, baseTableSchemaName, baseTableCatalogName
	}

	/**   */
	
	void dropForeignKeyConstraint(Map<String, Object> params) {
		addMapBasedChange Tag.dropForeignKeyConstraint, params
	}

	/**   */
	void dropAllForeignKeyConstraints( String baseTableName, String baseTableSchemaName=null, String baseTableCatalogName=null) {
		addChange Tag.dropAllForeignKeyConstraints, baseTableName, baseTableSchemaName, baseTableCatalogName
	}

	/**   */
	void dropAllForeignKeyConstraints(Map<String, Object> namedArgs, String baseTableName, String baseTableSchemaName=null, String baseTableCatalogName=null) {
		addChange Tag.dropAllForeignKeyConstraints, namedArgs, baseTableName, baseTableSchemaName, baseTableCatalogName
	}

	/**   */
	
	void dropAllForeignKeyConstraints(Map<String, Object> params) {
		addMapBasedChange Tag.dropAllForeignKeyConstraints, params
	}

	/**   */
	void addPrimaryKey( String columnNames, String tableName, String schemaName=null, String catalogName=null, String constraintName=null, String tablespace=null, Boolean clustered=null, String forIndexName=null, String forIndexSchemaName=null, String forIndexCatalogName=null, Boolean validate=null) {
		addChange Tag.addPrimaryKey, columnNames, tableName, schemaName, catalogName, constraintName, tablespace, clustered, forIndexName, forIndexSchemaName, forIndexCatalogName, validate
	}

	/**   */
	void addPrimaryKey(Map<String, Object> namedArgs, String columnNames, String tableName, String schemaName=null, String catalogName=null, String constraintName=null, String tablespace=null, Boolean clustered=null, String forIndexName=null, String forIndexSchemaName=null, String forIndexCatalogName=null, Boolean validate=null) {
		addChange Tag.addPrimaryKey, namedArgs, columnNames, tableName, schemaName, catalogName, constraintName, tablespace, clustered, forIndexName, forIndexSchemaName, forIndexCatalogName, validate
	}

	/**   */
	
	void addPrimaryKey(Map<String, Object> params) {
		addMapBasedChange Tag.addPrimaryKey, params
	}

	/**   */
	void dropPrimaryKey( String constraintName=null, String tableName, String schemaName=null, String catalogName=null, Boolean dropIndex=null) {
		addChange Tag.dropPrimaryKey, constraintName, tableName, schemaName, catalogName, dropIndex
	}

	/**   */
	void dropPrimaryKey(Map<String, Object> namedArgs, String constraintName=null, String tableName, String schemaName=null, String catalogName=null, Boolean dropIndex=null) {
		addChange Tag.dropPrimaryKey, namedArgs, constraintName, tableName, schemaName, catalogName, dropIndex
	}

	/**   */
	
	void dropPrimaryKey(Map<String, Object> params) {
		addMapBasedChange Tag.dropPrimaryKey, params
	}

	/**   */
	void addLookupTable( String existingTableCatalogName=null, String existingTableSchemaName=null, String existingTableName, String existingColumnName, String newTableCatalogName=null, String newTableSchemaName=null, String newTableName, String newColumnName, String newColumnDataType=null, String constraintName=null) {
		addChange Tag.addLookupTable, existingTableCatalogName, existingTableSchemaName, existingTableName, existingColumnName, newTableCatalogName, newTableSchemaName, newTableName, newColumnName, newColumnDataType, constraintName
	}

	/**   */
	void addLookupTable(Map<String, Object> namedArgs, String existingTableCatalogName=null, String existingTableSchemaName=null, String existingTableName, String existingColumnName, String newTableCatalogName=null, String newTableSchemaName=null, String newTableName, String newColumnName, String newColumnDataType=null, String constraintName=null) {
		addChange Tag.addLookupTable, namedArgs, existingTableCatalogName, existingTableSchemaName, existingTableName, existingColumnName, newTableCatalogName, newTableSchemaName, newTableName, newColumnName, newColumnDataType, constraintName
	}

	/**   */
	
	void addLookupTable(Map<String, Object> params) {
		addMapBasedChange Tag.addLookupTable, params
	}

	/**   */
	void addAutoIncrement( String columnName, String tableName, String columnDataType=null, Long startWith=null, Long incrementBy=null, Boolean defaultOnNull=null, String generationType=null, String schemaName=null, String catalogName=null) {
		addChange Tag.addAutoIncrement, columnName, tableName, columnDataType, startWith, incrementBy, defaultOnNull, generationType, schemaName, catalogName
	}

	/**   */
	void addAutoIncrement(Map<String, Object> namedArgs, String columnName, String tableName, String columnDataType=null, Long startWith=null, Long incrementBy=null, Boolean defaultOnNull=null, String generationType=null, String schemaName=null, String catalogName=null) {
		addChange Tag.addAutoIncrement, namedArgs, columnName, tableName, columnDataType, startWith, incrementBy, defaultOnNull, generationType, schemaName, catalogName
	}

	/**   */
	
	void addAutoIncrement(Map<String, Object> params) {
		addMapBasedChange Tag.addAutoIncrement, params
	}

	/**   */
	void addDefaultValue( String columnName, String tableName, String defaultValue=null, String defaultValueNumeric=null, String defaultValueDate=null, String defaultValueBoolean=null, String defaultValueComputed=null, String defaultValueSequenceNext=null, String defaultValueConstraintName=null, String columnDataType=null, String schemaName=null, String catalogName=null) {
		addChange Tag.addDefaultValue, columnName, tableName, defaultValue, defaultValueNumeric, defaultValueDate, defaultValueBoolean, defaultValueComputed, defaultValueSequenceNext, defaultValueConstraintName, columnDataType, schemaName, catalogName
	}

	/**   */
	void addDefaultValue(Map<String, Object> namedArgs, String columnName, String tableName, String defaultValue=null, String defaultValueNumeric=null, String defaultValueDate=null, String defaultValueBoolean=null, String defaultValueComputed=null, String defaultValueSequenceNext=null, String defaultValueConstraintName=null, String columnDataType=null, String schemaName=null, String catalogName=null) {
		addChange Tag.addDefaultValue, namedArgs, columnName, tableName, defaultValue, defaultValueNumeric, defaultValueDate, defaultValueBoolean, defaultValueComputed, defaultValueSequenceNext, defaultValueConstraintName, columnDataType, schemaName, catalogName
	}

	/**   */
	
	void addDefaultValue(Map<String, Object> params) {
		addMapBasedChange Tag.addDefaultValue, params
	}

	/**   */
	void dropDefaultValue( String columnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null) {
		addChange Tag.dropDefaultValue, columnName, tableName, schemaName, catalogName, columnDataType
	}

	/**   */
	void dropDefaultValue(Map<String, Object> namedArgs, String columnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null) {
		addChange Tag.dropDefaultValue, namedArgs, columnName, tableName, schemaName, catalogName, columnDataType
	}

	/**   */
	
	void dropDefaultValue(Map<String, Object> params) {
		addMapBasedChange Tag.dropDefaultValue, params
	}

	/**   */
	void addUniqueConstraint( String columnNames, String tableName, String schemaName=null, String catalogName=null, String constraintName=null, String tablespace=null, Boolean disabled=null, Boolean deferrable=null, Boolean initiallyDeferred=null, String forIndexCatalogName=null, String forIndexSchemaName=null, String forIndexName=null, Boolean clustered=null, Boolean validate=null) {
		addChange Tag.addUniqueConstraint, columnNames, tableName, schemaName, catalogName, constraintName, tablespace, disabled, deferrable, initiallyDeferred, forIndexCatalogName, forIndexSchemaName, forIndexName, clustered, validate
	}

	/**   */
	void addUniqueConstraint(Map<String, Object> namedArgs, String columnNames, String tableName, String schemaName=null, String catalogName=null, String constraintName=null, String tablespace=null, Boolean disabled=null, Boolean deferrable=null, Boolean initiallyDeferred=null, String forIndexCatalogName=null, String forIndexSchemaName=null, String forIndexName=null, Boolean clustered=null, Boolean validate=null) {
		addChange Tag.addUniqueConstraint, namedArgs, columnNames, tableName, schemaName, catalogName, constraintName, tablespace, disabled, deferrable, initiallyDeferred, forIndexCatalogName, forIndexSchemaName, forIndexName, clustered, validate
	}

	/**   */
	
	void addUniqueConstraint(Map<String, Object> params) {
		addMapBasedChange Tag.addUniqueConstraint, params
	}

	/**   */
	void dropUniqueConstraint( String constraintName, String tableName, String schemaName=null, String catalogName=null, String uniqueColumns=null) {
		addChange Tag.dropUniqueConstraint, constraintName, tableName, schemaName, catalogName, uniqueColumns
	}

	/**   */
	void dropUniqueConstraint(Map<String, Object> namedArgs, String constraintName, String tableName, String schemaName=null, String catalogName=null, String uniqueColumns=null) {
		addChange Tag.dropUniqueConstraint, namedArgs, constraintName, tableName, schemaName, catalogName, uniqueColumns
	}

	/**   */
	
	void dropUniqueConstraint(Map<String, Object> params) {
		addMapBasedChange Tag.dropUniqueConstraint, params
	}

	/**   */
	void setTableRemarks( String remarks, String tableName, String schemaName=null, String catalogName=null) {
		addChange Tag.setTableRemarks, remarks, tableName, schemaName, catalogName
	}

	/**   */
	void setTableRemarks(Map<String, Object> namedArgs, String remarks, String tableName, String schemaName=null, String catalogName=null) {
		addChange Tag.setTableRemarks, namedArgs, remarks, tableName, schemaName, catalogName
	}

	/**   */
	
	void setTableRemarks(Map<String, Object> params) {
		addMapBasedChange Tag.setTableRemarks, params
	}

	/**   */
	void setColumnRemarks( String remarks, String columnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null, ColumnParentTypeEnum columnParentType=null) {
		addChange Tag.setColumnRemarks, remarks, columnName, tableName, schemaName, catalogName, columnDataType, columnParentType
	}

	/**   */
	void setColumnRemarks(Map<String, Object> namedArgs, String remarks, String columnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null, ColumnParentTypeEnum columnParentType=null) {
		addChange Tag.setColumnRemarks, namedArgs, remarks, columnName, tableName, schemaName, catalogName, columnDataType, columnParentType
	}

	/**   */
	void setColumnRemarks(Map<String, Object> params) {
		addMapBasedChange Tag.setColumnRemarks, params
	}

	/** Update data in the specified table  */
	void update( String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=UpdateDelegate, strategy=DELEGATE_ONLY) Closure closure) {
		addChange Tag.update, tableName, schemaName, catalogName, closure
	}

	/** Update data in the specified table  */
	void update(Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=UpdateDelegate, strategy=DELEGATE_ONLY) Closure closure) {
		addChange Tag.update, namedArgs, tableName, schemaName, catalogName, closure
	}

	/** Update data in the specified table  */
	
	void update(Map<String, Object> params, 
				@DelegatesTo(value=UpdateDelegate, strategy=DELEGATE_ONLY) Closure closure) {
		addChangeWithChild Tag.update, params, closure
	}
	
	/**   */
	void delete( String tableName, String schemaName=null, String catalogName=null) {
		addChange Tag.delete, tableName, schemaName, catalogName
	}

	/**   */
	void delete(Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null) {
		addChange Tag.delete, namedArgs, tableName, schemaName, catalogName
	}

	/**   */
	void delete( String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=DeleteDelegate, strategy=DELEGATE_ONLY) Closure closure) {
		addChange Tag.delete, tableName, schemaName, catalogName, closure
	}

	
	/**   */
	void delete(Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=DeleteDelegate, strategy=DELEGATE_ONLY) Closure closure) {
		addChange Tag.delete, namedArgs, tableName, schemaName, catalogName, closure
	}

	/**   */
	
	void delete(Map<String, Object> params, 
				@DelegatesTo(value=DeleteDelegate, strategy=DELEGATE_ONLY) Closure closure=null) {
		addChangeWithChild Tag.delete, params, closure
	}
	
	/** Load data from a CSV defined {@code file} into an existing table defined by {@code tableName.}
		Nested column tags can define type, default value and name mapping if columns in the CSV have different name as the table column needs to be loaded into
		All CSV columns are used by default while generating SQL even if they are not described by a column 
	 <br>Params:<dl>
	 <dt><b>tableName</b></dt>
		<dd>Name of the table to insert data into</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the file name defined in {@code file} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>separator</dt>
		<dd>Character separating the fields. Default: ','</dd>
	</dl> */
	void loadData( String tableName, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null) {
		addChange Tag.loadData, tableName, file, relativeToChangelogFile, encoding, separator, quotchar, commentLineStartsWith, usePreparedStatements, schemaName, catalogName
	}

	/** Load data from a CSV defined {@code file} into an existing table defined by {@code tableName.}
		Nested column tags can define type, default value and name mapping if columns in the CSV have different name as the table column needs to be loaded into
		All CSV columns are used by default while generating SQL even if they are not described by a column 
	 <br>Params:<dl>
	 <dt><b>tableName</b></dt>
		<dd>Name of the table to insert data into</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the file name defined in {@code file} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>separator</dt>
		<dd>Character separating the fields. Default: ','</dd>
	</dl> */
	void loadData(Map<String, Object> namedArgs, String tableName, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null) {
		addChange Tag.loadData, namedArgs, tableName, file, relativeToChangelogFile, encoding, separator, quotchar, commentLineStartsWith, usePreparedStatements, schemaName, catalogName
	}

	/** Load data from a CSV defined {@code file} into an existing table defined by {@code tableName.}
		Nested column tags can define type, default value and name mapping if columns in the CSV have different name as the table column needs to be loaded into
		All CSV columns are used by default while generating SQL even if they are not described by a column 
	 <br>Params:<dl>
	 <dt><b>tableName</b></dt>
		<dd>Name of the table to insert data into</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the file name defined in {@code file} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>separator</dt>
		<dd>Character separating the fields. Default: ','</dd>
	</dl> */
	void loadData( String tableName, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=LoadDataDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.loadData, tableName, file, relativeToChangelogFile, encoding, separator, quotchar, commentLineStartsWith, usePreparedStatements, schemaName, catalogName, columns
	}

	
	/** Load data from a CSV defined {@code file} into an existing table defined by {@code tableName.}
		Nested column tags can define type, default value and name mapping if columns in the CSV have different name as the table column needs to be loaded into
		All CSV columns are used by default while generating SQL even if they are not described by a column 
	 <br>Params:<dl>
	 <dt><b>tableName</b></dt>
		<dd>Name of the table to insert data into</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the file name defined in {@code file} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>separator</dt>
		<dd>Character separating the fields. Default: ','</dd>
	</dl> */
	void loadData(Map<String, Object> namedArgs, String tableName, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=LoadDataDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.loadData, namedArgs, tableName, file, relativeToChangelogFile, encoding, separator, quotchar, commentLineStartsWith, usePreparedStatements, schemaName, catalogName, columns
	}

	/** Load data from a CSV defined {@code file} into an existing table defined by {@code tableName.}
		Nested column tags can define type, default value and name mapping if columns in the CSV have different name as the table column needs to be loaded into
		All CSV columns are used by default while generating SQL even if they are not described by a column 
	 <br>Params:<dl>
	 <dt><b>tableName</b></dt>
		<dd>Name of the table to insert data into</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the file name defined in {@code file} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>separator</dt>
		<dd>Character separating the fields. Default: ','</dd>
	</dl> */
	
	void loadData(Map<String, Object> params, 
				@DelegatesTo(value=LoadDataDelegate, strategy=DELEGATE_ONLY) Closure columns=null) {
		addChangeWithChild Tag.loadData, params, columns
	}
	
	/** Loads or updates data from a CSV file into an existing table.
		Differs from loadData by issuing a SQL batch that checks for the existence of a record. If found, the record is UPDATEd, else the record is INSERTed 
	 <br>Params:<dl>
	 <dt><b>tableName</b></dt>
		<dd>Name of the table to insert or update data in</dd>
	 <dt><b>primaryKey</b></dt>
		<dd>Comma delimited list of the columns for the primary key used to identify existing rows</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the file name defined in {@code file} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>separator</dt>
		<dd>Character separating the fields. Default: ','</dd>
	 <dt>onlyUpdate</dt>
		<dd>If true, records with no matching database record should be ignored</dd>
	</dl> */
	void loadUpdateData( String tableName, String primaryKey, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null, Boolean onlyUpdate=null) {
		addChange Tag.loadUpdateData, tableName, primaryKey, file, relativeToChangelogFile, encoding, separator, quotchar, commentLineStartsWith, usePreparedStatements, schemaName, catalogName, onlyUpdate
	}

	/** Loads or updates data from a CSV file into an existing table.
		Differs from loadData by issuing a SQL batch that checks for the existence of a record. If found, the record is UPDATEd, else the record is INSERTed 
	 <br>Params:<dl>
	 <dt><b>tableName</b></dt>
		<dd>Name of the table to insert or update data in</dd>
	 <dt><b>primaryKey</b></dt>
		<dd>Comma delimited list of the columns for the primary key used to identify existing rows</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the file name defined in {@code file} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>separator</dt>
		<dd>Character separating the fields. Default: ','</dd>
	 <dt>onlyUpdate</dt>
		<dd>If true, records with no matching database record should be ignored</dd>
	</dl> */
	void loadUpdateData(Map<String, Object> namedArgs, String tableName, String primaryKey, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null, Boolean onlyUpdate=null) {
		addChange Tag.loadUpdateData, namedArgs, tableName, primaryKey, file, relativeToChangelogFile, encoding, separator, quotchar, commentLineStartsWith, usePreparedStatements, schemaName, catalogName, onlyUpdate
	}

	/** Loads or updates data from a CSV file into an existing table.
		Differs from loadData by issuing a SQL batch that checks for the existence of a record. If found, the record is UPDATEd, else the record is INSERTed 
	 <br>Params:<dl>
	 <dt><b>tableName</b></dt>
		<dd>Name of the table to insert or update data in</dd>
	 <dt><b>primaryKey</b></dt>
		<dd>Comma delimited list of the columns for the primary key used to identify existing rows</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the file name defined in {@code file} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>separator</dt>
		<dd>Character separating the fields. Default: ','</dd>
	 <dt>onlyUpdate</dt>
		<dd>If true, records with no matching database record should be ignored</dd>
	</dl> */
	void loadUpdateData( String tableName, String primaryKey, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null, Boolean onlyUpdate=null, 
				@DelegatesTo(value=LoadDataDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.loadUpdateData, tableName, primaryKey, file, relativeToChangelogFile, encoding, separator, quotchar, commentLineStartsWith, usePreparedStatements, schemaName, catalogName, onlyUpdate, columns
	}

	
	/** Loads or updates data from a CSV file into an existing table.
		Differs from loadData by issuing a SQL batch that checks for the existence of a record. If found, the record is UPDATEd, else the record is INSERTed 
	 <br>Params:<dl>
	 <dt><b>tableName</b></dt>
		<dd>Name of the table to insert or update data in</dd>
	 <dt><b>primaryKey</b></dt>
		<dd>Comma delimited list of the columns for the primary key used to identify existing rows</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the file name defined in {@code file} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>separator</dt>
		<dd>Character separating the fields. Default: ','</dd>
	 <dt>onlyUpdate</dt>
		<dd>If true, records with no matching database record should be ignored</dd>
	</dl> */
	void loadUpdateData(Map<String, Object> namedArgs, String tableName, String primaryKey, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null, Boolean onlyUpdate=null, 
				@DelegatesTo(value=LoadDataDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChange Tag.loadUpdateData, namedArgs, tableName, primaryKey, file, relativeToChangelogFile, encoding, separator, quotchar, commentLineStartsWith, usePreparedStatements, schemaName, catalogName, onlyUpdate, columns
	}

	/** Loads or updates data from a CSV file into an existing table.
		Differs from loadData by issuing a SQL batch that checks for the existence of a record. If found, the record is UPDATEd, else the record is INSERTed 
	 <br>Params:<dl>
	 <dt><b>tableName</b></dt>
		<dd>Name of the table to insert or update data in</dd>
	 <dt><b>primaryKey</b></dt>
		<dd>Comma delimited list of the columns for the primary key used to identify existing rows</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the file name defined in {@code file} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>separator</dt>
		<dd>Character separating the fields. Default: ','</dd>
	 <dt>onlyUpdate</dt>
		<dd>If true, records with no matching database record should be ignored</dd>
	</dl> */
	void loadUpdateData(Map<String, Object> params,
				@DelegatesTo(value=LoadDataDelegate, strategy=DELEGATE_ONLY) Closure columns=null) {
		addChangeWithChild Tag.loadUpdateData, params, columns
	}
	
	/** Execute a shell command.  */
	void executeCommand( String executable, String os=null, String timeout=null) {
		addChange Tag.executeCommand, executable, os, timeout
	}

	/** Execute a shell command.  */
	void executeCommand(Map<String, Object> namedArgs, String executable, String os=null, String timeout=null) {
		addChange Tag.executeCommand, namedArgs, executable, os, timeout
	}

	/** Execute a shell command.  */
	void executeCommand( String executable, String os=null, String timeout=null, 
				@DelegatesTo(value=ArgumentDelegate, strategy=DELEGATE_ONLY) Closure args) {
		addChange Tag.executeCommand, executable, os, timeout, args
	}

	
	/** Execute a shell command.  */
	void executeCommand(Map<String, Object> namedArgs, String executable, String os=null, String timeout=null, 
				@DelegatesTo(value=ArgumentDelegate, strategy=DELEGATE_ONLY) Closure args) {
		addChange Tag.executeCommand, namedArgs, executable, os, timeout, args
	}

	/** Execute a shell command.  */
	
	void executeCommand(Map<String, Object> params, 
				@DelegatesTo(value=ArgumentDelegate, strategy=DELEGATE_ONLY) Closure args=null) {
		addChangeWithChild Tag.executeCommand, params, args
	}
	
}
