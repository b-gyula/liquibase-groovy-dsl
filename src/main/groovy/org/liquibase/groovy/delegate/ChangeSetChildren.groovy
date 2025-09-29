package org.liquibase.groovy.delegate
/* Generated @ Sat Sep 27 15:34:45 CEST 2025 on M600 */
import groovy.transform.CompileStatic
import groovy.transform.SelfType
import liquibase.database.ColumnParentTypeEnum
import liquibase.database.FkCascadeActionOptions
import static org.liquibase.groovy.delegate.ChangeSetDelegate.*
import static groovy.lang.Closure.DELEGATE_ONLY

@CompileStatic
@SelfType(ChangeSetDelegate)
trait ChangeSetChildren {

	/** Create a table with the defined columns */
	void createTable( String tableName, Boolean ifNotExists=null, String schemaName=null, String catalogName=null, String tablespace=null, String tableType=null, String remarks=null, Boolean rowDependencies=null, 
				@DelegatesTo(value=CreateTableDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		createTable chkMap(Tag.createTable) ('tableName',tableName) ('ifNotExists',ifNotExists) ('schemaName',schemaName) ('catalogName',catalogName) ('tablespace',tablespace) ('tableType',tableType) ('remarks',remarks) ('rowDependencies',rowDependencies) .asMap, columns
	}

	/** Create a table with the defined columns */
	void createTable( Map<String, Object> namedArgs, String tableName, Boolean ifNotExists=null, String schemaName=null, String catalogName=null, String tablespace=null, String tableType=null, String remarks=null, Boolean rowDependencies=null, 
				@DelegatesTo(value=CreateTableDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		createTable chkMap(Tag.createTable,namedArgs) ('tableName',tableName) ('ifNotExists',ifNotExists) ('schemaName',schemaName) ('catalogName',catalogName) ('tablespace',tablespace) ('tableType',tableType) ('remarks',remarks) ('rowDependencies',rowDependencies) .asMap, columns
	}

	/** Create a table with the defined columns */
	void createTable(Map<String, Object> params, 
				@DelegatesTo(value=CreateTableDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChangeWithChild Tag.createTable, params, columns
	}

	/**  */
	void dropTable( String tableName, Boolean cascadeConstraints=null, String schemaName=null, String catalogName=null) {
		dropTable chkMap(Tag.dropTable) ('tableName',tableName) ('cascadeConstraints',cascadeConstraints) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void dropTable( Map<String, Object> namedArgs, String tableName, Boolean cascadeConstraints=null, String schemaName=null, String catalogName=null) {
		dropTable chkMap(Tag.dropTable,namedArgs) ('tableName',tableName) ('cascadeConstraints',cascadeConstraints) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void dropTable(Map<String, Object> params) {
		addChange Tag.dropTable, params
	}

	/** 
	 <br>Params:<dl>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the path defined in {@code path} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	</dl> */
	void createView( String viewName, String path, Boolean replaceIfExists=null, Boolean fullDefinition=null, Boolean relativeToChangelogFile=null, String remarks=null, String encoding=null, String schemaName=null, String catalogName=null) {
		createView chkMap(Tag.createView) ('viewName',viewName) ('path',path) ('replaceIfExists',replaceIfExists) ('fullDefinition',fullDefinition) ('relativeToChangelogFile',relativeToChangelogFile) ('remarks',remarks) ('encoding',encoding) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/** 
	 <br>Params:<dl>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the path defined in {@code path} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	</dl> */
	void createView( Map<String, Object> namedArgs, String viewName, String path, Boolean replaceIfExists=null, Boolean fullDefinition=null, Boolean relativeToChangelogFile=null, String remarks=null, String encoding=null, String schemaName=null, String catalogName=null) {
		createView chkMap(Tag.createView,namedArgs) ('viewName',viewName) ('path',path) ('replaceIfExists',replaceIfExists) ('fullDefinition',fullDefinition) ('relativeToChangelogFile',relativeToChangelogFile) ('remarks',remarks) ('encoding',encoding) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/** 
	 <br>Params:<dl>
	 <dt>selectQuery</dt>
		<dd>SQL generating the view</dd>
	</dl> */
	void createView( String viewName, Boolean replaceIfExists=null, Boolean fullDefinition=null, String remarks=null, String schemaName=null, String catalogName=null, Closure<String> selectQuery) {
		createView chkMap(Tag.createView) ('viewName',viewName) ('replaceIfExists',replaceIfExists) ('fullDefinition',fullDefinition) ('remarks',remarks) ('schemaName',schemaName) ('catalogName',catalogName) ('selectQuery',selectQuery ? selectQuery() as String: null).asMap
	}

	/** 
	 <br>Params:<dl>
	 <dt>selectQuery</dt>
		<dd>SQL generating the view</dd>
	</dl> */
	void createView( Map<String, Object> namedArgs, String viewName, Boolean replaceIfExists=null, Boolean fullDefinition=null, String remarks=null, String schemaName=null, String catalogName=null, Closure<String> selectQuery) {
		createView chkMap(Tag.createView,namedArgs) ('viewName',viewName) ('replaceIfExists',replaceIfExists) ('fullDefinition',fullDefinition) ('remarks',remarks) ('schemaName',schemaName) ('catalogName',catalogName) ('selectQuery',selectQuery ? selectQuery() as String: null).asMap
	}

	/** 
	 <br>Params:<dl>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the path defined in {@code path} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>selectQuery</dt>
		<dd>SQL generating the view</dd>
	</dl> */
	void createView(Map<String, Object> params, Closure<String> selectQuery=null) {
		addChange chkMap(Tag.createView, params)('selectQuery',selectQuery ? selectQuery() as String: null)
	}
		
	/**  */
	void renameView( String oldViewName, String newViewName, String schemaName=null, String catalogName=null) {
		renameView chkMap(Tag.renameView) ('oldViewName',oldViewName) ('newViewName',newViewName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void renameView( Map<String, Object> namedArgs, String oldViewName, String newViewName, String schemaName=null, String catalogName=null) {
		renameView chkMap(Tag.renameView,namedArgs) ('oldViewName',oldViewName) ('newViewName',newViewName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void renameView(Map<String, Object> params) {
		addChange Tag.renameView, params
	}

	/**  */
	void dropView( String viewName, Boolean ifExists=null, String schemaName=null, String catalogName=null) {
		dropView chkMap(Tag.dropView) ('viewName',viewName) ('ifExists',ifExists) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void dropView( Map<String, Object> namedArgs, String viewName, Boolean ifExists=null, String schemaName=null, String catalogName=null) {
		dropView chkMap(Tag.dropView,namedArgs) ('viewName',viewName) ('ifExists',ifExists) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void dropView(Map<String, Object> params) {
		addChange Tag.dropView, params
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
				@DelegatesTo(value=InsertDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		insert chkMap(Tag.insert) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('dbms',dbms) .asMap, columns
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
	void insert( Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null, String dbms=null, 
				@DelegatesTo(value=InsertDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		insert chkMap(Tag.insert,namedArgs) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('dbms',dbms) .asMap, columns
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
				@DelegatesTo(value=InsertDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChangeWithChild Tag.insert, params, columns
	}

	/**  */
	void addColumn( String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=AddColumnDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addColumn chkMap(Tag.addColumn) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, columns
	}

	/**  */
	void addColumn( Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=AddColumnDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addColumn chkMap(Tag.addColumn,namedArgs) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, columns
	}

	/**  */
	void addColumn(Map<String, Object> params, 
				@DelegatesTo(value=AddColumnDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChangeWithChild Tag.addColumn, params, columns
	}

	/** Create a definition for a stored procedure from either the tag content or from file defined by {@code path.}
	 <br>Params:<dl>
	 <dt><b>path</b></dt>
		<dd>File containing the procedure text. Either this attribute or a nested procedure text is required.</dd>
	 <dt>procedureName</dt>
		<dd>Name of the stored procedure. Required if replaceIfExists=true.</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the path defined in {@code path} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>replaceIfExists</dt>
		<dd>If the stored procedure defined by {@code procedureName} already exists, alter it instead of creating it. Default: false.</dd>
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	</dl> */
	void createProcedure( String path, String procedureName=null, Boolean relativeToChangelogFile=null, Boolean replaceIfExists=null, String dbms=null, String encoding=null, String schemaName=null, String catalogName=null) {
		createProcedure chkMap(Tag.createProcedure) ('path',path) ('procedureName',procedureName) ('relativeToChangelogFile',relativeToChangelogFile) ('replaceIfExists',replaceIfExists) ('dbms',dbms) ('encoding',encoding) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/** Create a definition for a stored procedure from either the tag content or from file defined by {@code path.}
	 <br>Params:<dl>
	 <dt><b>path</b></dt>
		<dd>File containing the procedure text. Either this attribute or a nested procedure text is required.</dd>
	 <dt>procedureName</dt>
		<dd>Name of the stored procedure. Required if replaceIfExists=true.</dd>
	 <dt>relativeToChangelogFile</dt>
		<dd>Specifies whether the path defined in {@code path} is relative to the
		changelog file rather than looked up in the search path. Default: false
		See: https://docs.liquibase.com/concepts/changelogs/how-liquibase-finds-files.html</dd>
	 <dt>replaceIfExists</dt>
		<dd>If the stored procedure defined by {@code procedureName} already exists, alter it instead of creating it. Default: false.</dd>
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	</dl> */
	void createProcedure( Map<String, Object> namedArgs, String path, String procedureName=null, Boolean relativeToChangelogFile=null, Boolean replaceIfExists=null, String dbms=null, String encoding=null, String schemaName=null, String catalogName=null) {
		createProcedure chkMap(Tag.createProcedure,namedArgs) ('path',path) ('procedureName',procedureName) ('relativeToChangelogFile',relativeToChangelogFile) ('replaceIfExists',replaceIfExists) ('dbms',dbms) ('encoding',encoding) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/** Create a definition for a stored procedure from either the tag content or from file defined by {@code path.}
	 <br>Params:<dl>
	 <dt>procedureName</dt>
		<dd>Name of the stored procedure. Required if replaceIfExists=true.</dd>
	 <dt>replaceIfExists</dt>
		<dd>If the stored procedure defined by {@code procedureName} already exists, alter it instead of creating it. Default: false.</dd>
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	 <dt>procedureText</dt>
		<dd>The SQL creating the procedure.</dd>
	</dl> */
	void createProcedure( String procedureName=null, Boolean replaceIfExists=null, String dbms=null, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=CreateProcedureDelegate, strategy=DELEGATE_ONLY) Closure procedureText) {
		createProcedure chkMap(Tag.createProcedure) ('procedureName',procedureName) ('replaceIfExists',replaceIfExists) ('dbms',dbms) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, procedureText
	}

	/** Create a definition for a stored procedure from either the tag content or from file defined by {@code path.}
	 <br>Params:<dl>
	 <dt>procedureName</dt>
		<dd>Name of the stored procedure. Required if replaceIfExists=true.</dd>
	 <dt>replaceIfExists</dt>
		<dd>If the stored procedure defined by {@code procedureName} already exists, alter it instead of creating it. Default: false.</dd>
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	 <dt>procedureText</dt>
		<dd>The SQL creating the procedure.</dd>
	</dl> */
	void createProcedure( Map<String, Object> namedArgs, String procedureName=null, Boolean replaceIfExists=null, String dbms=null, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=CreateProcedureDelegate, strategy=DELEGATE_ONLY) Closure procedureText) {
		createProcedure chkMap(Tag.createProcedure,namedArgs) ('procedureName',procedureName) ('replaceIfExists',replaceIfExists) ('dbms',dbms) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, procedureText
	}

	/**  */
	void dropProcedure( String procedureName, String schemaName=null, String catalogName=null) {
		dropProcedure chkMap(Tag.dropProcedure) ('procedureName',procedureName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void dropProcedure( Map<String, Object> namedArgs, String procedureName, String schemaName=null, String catalogName=null) {
		dropProcedure chkMap(Tag.dropProcedure,namedArgs) ('procedureName',procedureName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void dropProcedure(Map<String, Object> params) {
		addChange Tag.dropProcedure, params
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
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	 <dt>splitStatements</dt>
		<dd>When true, Liquibase splits statements on {@code endDelimiter} and executes them separately. Default: true.</dd>
	 <dt>endDelimiter</dt>
		<dd>The delimiter to separate raw SQL statements. The default value is `;`
		See: https://docs.liquibase.com/change-types/enddelimiter-sql.html</dd>
	</dl> */
	void sqlFile( String path, Boolean relativeToChangelogFile=null, Boolean stripComments=null, String dbms=null, Boolean splitStatements=null, String endDelimiter=null, String encoding=null) {
		sqlFile chkMap(Tag.sqlFile) ('path',path) ('relativeToChangelogFile',relativeToChangelogFile) ('stripComments',stripComments) ('dbms',dbms) ('splitStatements',splitStatements) ('endDelimiter',endDelimiter) ('encoding',encoding).asMap
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
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	 <dt>splitStatements</dt>
		<dd>When true, Liquibase splits statements on {@code endDelimiter} and executes them separately. Default: true.</dd>
	 <dt>endDelimiter</dt>
		<dd>The delimiter to separate raw SQL statements. The default value is `;`
		See: https://docs.liquibase.com/change-types/enddelimiter-sql.html</dd>
	</dl> */
	void sqlFile( Map<String, Object> namedArgs, String path, Boolean relativeToChangelogFile=null, Boolean stripComments=null, String dbms=null, Boolean splitStatements=null, String endDelimiter=null, String encoding=null) {
		sqlFile chkMap(Tag.sqlFile,namedArgs) ('path',path) ('relativeToChangelogFile',relativeToChangelogFile) ('stripComments',stripComments) ('dbms',dbms) ('splitStatements',splitStatements) ('endDelimiter',endDelimiter) ('encoding',encoding).asMap
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
	 <dt>dbms</dt>
		<dd>Specifies which database type(s) a changeset is to be used for.
		See valid database type names on Liquibase Database Tutorials
		. Separate multiple databases with commas. Specify that a changeset is not applicable to a particular
		database type by prefixing with !. The keywords all and none are also available.
		Will run for all dbms' if empty or absent</dd>
	 <dt>splitStatements</dt>
		<dd>When true, Liquibase splits statements on {@code endDelimiter} and executes them separately. Default: true.</dd>
	 <dt>endDelimiter</dt>
		<dd>The delimiter to separate raw SQL statements. The default value is `;`
		See: https://docs.liquibase.com/change-types/enddelimiter-sql.html</dd>
	</dl> */
	void sqlFile(Map<String, Object> params) {
		addChange Tag.sqlFile, params
	}

	/**  */
	void renameTable( String oldTableName, String newTableName, String schemaName=null, String catalogName=null) {
		renameTable chkMap(Tag.renameTable) ('oldTableName',oldTableName) ('newTableName',newTableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void renameTable( Map<String, Object> namedArgs, String oldTableName, String newTableName, String schemaName=null, String catalogName=null) {
		renameTable chkMap(Tag.renameTable,namedArgs) ('oldTableName',oldTableName) ('newTableName',newTableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void renameTable(Map<String, Object> params) {
		addChange Tag.renameTable, params
	}

	/**  */
	void renameColumn( String oldColumnName, String newColumnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null, String remarks=null) {
		renameColumn chkMap(Tag.renameColumn) ('oldColumnName',oldColumnName) ('newColumnName',newColumnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('columnDataType',columnDataType) ('remarks',remarks).asMap
	}

	/**  */
	void renameColumn( Map<String, Object> namedArgs, String oldColumnName, String newColumnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null, String remarks=null) {
		renameColumn chkMap(Tag.renameColumn,namedArgs) ('oldColumnName',oldColumnName) ('newColumnName',newColumnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('columnDataType',columnDataType) ('remarks',remarks).asMap
	}

	/**  */
	void renameColumn(Map<String, Object> params) {
		addChange Tag.renameColumn, params
	}

	/** Drop column(s). Either {@code columnName} or nested column(s) needs to be defined exclusively
	 <br>Params:<dl>
	 <dt><b>columnName</b></dt>
		<dd>Name of the column to drop, if dropping a single column. Ignore if nested columns are defined</dd>
	</dl> */
	void dropColumn( String columnName, String tableName, String schemaName=null, String catalogName=null) {
		dropColumn chkMap(Tag.dropColumn) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/** Drop column(s). Either {@code columnName} or nested column(s) needs to be defined exclusively
	 <br>Params:<dl>
	 <dt><b>columnName</b></dt>
		<dd>Name of the column to drop, if dropping a single column. Ignore if nested columns are defined</dd>
	</dl> */
	void dropColumn( Map<String, Object> namedArgs, String columnName, String tableName, String schemaName=null, String catalogName=null) {
		dropColumn chkMap(Tag.dropColumn,namedArgs) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/** Drop column(s). Either {@code columnName} or nested column(s) needs to be defined exclusively */
	void dropColumn( String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=DropColumnDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		dropColumn chkMap(Tag.dropColumn) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, columns
	}

	/** Drop column(s). Either {@code columnName} or nested column(s) needs to be defined exclusively */
	void dropColumn( Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=DropColumnDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		dropColumn chkMap(Tag.dropColumn,namedArgs) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, columns
	}

	/** Drop column(s). Either {@code columnName} or nested column(s) needs to be defined exclusively
	 <br>Params:<dl>
	 <dt><b>columnName</b></dt>
		<dd>Name of the column to drop, if dropping a single column. Ignore if nested columns are defined</dd>
	</dl> */
	void dropColumn(Map<String, Object> params, 
				@DelegatesTo(value=DropColumnDelegate, strategy=DELEGATE_ONLY) Closure columns=null) {
		addChangeWithChild Tag.dropColumn, params, columns
	}

	/**  */
	void mergeColumns( String column1Name, String joinString, String column2Name, String finalColumnName, String finalColumnType, String tableName, String schemaName=null, String catalogName=null) {
		mergeColumns chkMap(Tag.mergeColumns) ('column1Name',column1Name) ('joinString',joinString) ('column2Name',column2Name) ('finalColumnName',finalColumnName) ('finalColumnType',finalColumnType) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void mergeColumns( Map<String, Object> namedArgs, String column1Name, String joinString, String column2Name, String finalColumnName, String finalColumnType, String tableName, String schemaName=null, String catalogName=null) {
		mergeColumns chkMap(Tag.mergeColumns,namedArgs) ('column1Name',column1Name) ('joinString',joinString) ('column2Name',column2Name) ('finalColumnName',finalColumnName) ('finalColumnType',finalColumnType) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void mergeColumns(Map<String, Object> params) {
		addChange Tag.mergeColumns, params
	}

	/**  */
	void modifyDataType( String newDataType, String columnName, String tableName, String schemaName=null, String catalogName=null) {
		modifyDataType chkMap(Tag.modifyDataType) ('newDataType',newDataType) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void modifyDataType( Map<String, Object> namedArgs, String newDataType, String columnName, String tableName, String schemaName=null, String catalogName=null) {
		modifyDataType chkMap(Tag.modifyDataType,namedArgs) ('newDataType',newDataType) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void modifyDataType(Map<String, Object> params) {
		addChange Tag.modifyDataType, params
	}

	/** 
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	void createSequence( String sequenceName, Integer startValue=null, Integer incrementBy=null, Integer minValue=null, Integer maxValue=null, Boolean ordered=null, String cacheSize=null, String dataType=null, Boolean cycle=null, String schemaName=null, String catalogName=null) {
		createSequence chkMap(Tag.createSequence) ('sequenceName',sequenceName) ('startValue',startValue) ('incrementBy',incrementBy) ('minValue',minValue) ('maxValue',maxValue) ('ordered',ordered) ('cacheSize',cacheSize) ('dataType',dataType) ('cycle',cycle) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/** 
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	void createSequence( Map<String, Object> namedArgs, String sequenceName, Integer startValue=null, Integer incrementBy=null, Integer minValue=null, Integer maxValue=null, Boolean ordered=null, String cacheSize=null, String dataType=null, Boolean cycle=null, String schemaName=null, String catalogName=null) {
		createSequence chkMap(Tag.createSequence,namedArgs) ('sequenceName',sequenceName) ('startValue',startValue) ('incrementBy',incrementBy) ('minValue',minValue) ('maxValue',maxValue) ('ordered',ordered) ('cacheSize',cacheSize) ('dataType',dataType) ('cycle',cycle) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/** 
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	void createSequence(Map<String, Object> params) {
		addChange Tag.createSequence, params
	}

	/** 
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	void alterSequence( String sequenceName, Integer incrementBy=null, Integer minValue=null, Integer maxValue=null, Boolean ordered=null, String cacheSize=null, String dataType=null, Boolean cycle=null, String schemaName=null, String catalogName=null) {
		alterSequence chkMap(Tag.alterSequence) ('sequenceName',sequenceName) ('incrementBy',incrementBy) ('minValue',minValue) ('maxValue',maxValue) ('ordered',ordered) ('cacheSize',cacheSize) ('dataType',dataType) ('cycle',cycle) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/** 
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	void alterSequence( Map<String, Object> namedArgs, String sequenceName, Integer incrementBy=null, Integer minValue=null, Integer maxValue=null, Boolean ordered=null, String cacheSize=null, String dataType=null, Boolean cycle=null, String schemaName=null, String catalogName=null) {
		alterSequence chkMap(Tag.alterSequence,namedArgs) ('sequenceName',sequenceName) ('incrementBy',incrementBy) ('minValue',minValue) ('maxValue',maxValue) ('ordered',ordered) ('cacheSize',cacheSize) ('dataType',dataType) ('cycle',cycle) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/** 
	 <br>Params:<dl>
	 <dt>cycle</dt>
		<dd>true for a cycling sequence, false for a non-cycling sequence.
		Default: false.</dd>
	</dl> */
	void alterSequence(Map<String, Object> params) {
		addChange Tag.alterSequence, params
	}

	/**  */
	void dropSequence( String sequenceName, String schemaName=null, String catalogName=null) {
		dropSequence chkMap(Tag.dropSequence) ('sequenceName',sequenceName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void dropSequence( Map<String, Object> namedArgs, String sequenceName, String schemaName=null, String catalogName=null) {
		dropSequence chkMap(Tag.dropSequence,namedArgs) ('sequenceName',sequenceName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void dropSequence(Map<String, Object> params) {
		addChange Tag.dropSequence, params
	}

	/**  */
	void renameSequence( String oldSequenceName, String newSequenceName, String schemaName=null, String catalogName=null) {
		renameSequence chkMap(Tag.renameSequence) ('oldSequenceName',oldSequenceName) ('newSequenceName',newSequenceName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void renameSequence( Map<String, Object> namedArgs, String oldSequenceName, String newSequenceName, String schemaName=null, String catalogName=null) {
		renameSequence chkMap(Tag.renameSequence,namedArgs) ('oldSequenceName',oldSequenceName) ('newSequenceName',newSequenceName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void renameSequence(Map<String, Object> params) {
		addChange Tag.renameSequence, params
	}

	/**  */
	void createIndex( String indexName, String tableName, String schemaName=null, String catalogName=null, String associatedWith=null, Boolean unique=null, Boolean clustered=null, String tablespace=null, 
				@DelegatesTo(value=CreateIndexDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		createIndex chkMap(Tag.createIndex) ('indexName',indexName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('associatedWith',associatedWith) ('unique',unique) ('clustered',clustered) ('tablespace',tablespace) .asMap, columns
	}

	/**  */
	void createIndex( Map<String, Object> namedArgs, String indexName, String tableName, String schemaName=null, String catalogName=null, String associatedWith=null, Boolean unique=null, Boolean clustered=null, String tablespace=null, 
				@DelegatesTo(value=CreateIndexDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		createIndex chkMap(Tag.createIndex,namedArgs) ('indexName',indexName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('associatedWith',associatedWith) ('unique',unique) ('clustered',clustered) ('tablespace',tablespace) .asMap, columns
	}

	/**  */
	void createIndex(Map<String, Object> params, 
				@DelegatesTo(value=CreateIndexDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		addChangeWithChild Tag.createIndex, params, columns
	}

	/**  */
	void dropIndex( String indexName, String tableName, String schemaName=null, String catalogName=null, String associatedWith=null) {
		dropIndex chkMap(Tag.dropIndex) ('indexName',indexName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('associatedWith',associatedWith).asMap
	}

	/**  */
	void dropIndex( Map<String, Object> namedArgs, String indexName, String tableName, String schemaName=null, String catalogName=null, String associatedWith=null) {
		dropIndex chkMap(Tag.dropIndex,namedArgs) ('indexName',indexName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('associatedWith',associatedWith).asMap
	}

	/**  */
	void dropIndex(Map<String, Object> params) {
		addChange Tag.dropIndex, params
	}

	/**  */
	void addNotNullConstraint( String columnName, String tableName, String schemaName=null, String catalogName=null, String defaultNullValue=null, String columnDataType=null, String constraintName=null, Boolean validate=null) {
		addNotNullConstraint chkMap(Tag.addNotNullConstraint) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('defaultNullValue',defaultNullValue) ('columnDataType',columnDataType) ('constraintName',constraintName) ('validate',validate).asMap
	}

	/**  */
	void addNotNullConstraint( Map<String, Object> namedArgs, String columnName, String tableName, String schemaName=null, String catalogName=null, String defaultNullValue=null, String columnDataType=null, String constraintName=null, Boolean validate=null) {
		addNotNullConstraint chkMap(Tag.addNotNullConstraint,namedArgs) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('defaultNullValue',defaultNullValue) ('columnDataType',columnDataType) ('constraintName',constraintName) ('validate',validate).asMap
	}

	/**  */
	void addNotNullConstraint(Map<String, Object> params) {
		addChange Tag.addNotNullConstraint, params
	}

	/**  */
	void dropNotNullConstraint( String constraintName, String tableName, String schemaName=null, String catalogName=null, String columnName=null, String columnDataType=null) {
		dropNotNullConstraint chkMap(Tag.dropNotNullConstraint) ('constraintName',constraintName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('columnName',columnName) ('columnDataType',columnDataType).asMap
	}

	/**  */
	void dropNotNullConstraint( Map<String, Object> namedArgs, String constraintName, String tableName, String schemaName=null, String catalogName=null, String columnName=null, String columnDataType=null) {
		dropNotNullConstraint chkMap(Tag.dropNotNullConstraint,namedArgs) ('constraintName',constraintName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('columnName',columnName) ('columnDataType',columnDataType).asMap
	}

	/**  */
	void dropNotNullConstraint(Map<String, Object> params) {
		addChange Tag.dropNotNullConstraint, params
	}

	/**  */
	void addForeignKeyConstraint( String baseTableName, String baseColumnNames, String constraintName, String referencedTableName, String referencedColumnNames, String baseTableSchemaName=null, String baseTableCatalogName=null, String referencedTableSchemaName=null, String referencedTableCatalogName=null, Boolean deferrable=null, Boolean initiallyDeferred=null, Boolean deleteCascade=null, FkCascadeActionOptions onDelete=null, FkCascadeActionOptions onUpdate=null, Boolean referencesUniqueColumn=null, Boolean validate=null) {
		addForeignKeyConstraint chkMap(Tag.addForeignKeyConstraint) ('baseTableName',baseTableName) ('baseColumnNames',baseColumnNames) ('constraintName',constraintName) ('referencedTableName',referencedTableName) ('referencedColumnNames',referencedColumnNames) ('baseTableSchemaName',baseTableSchemaName) ('baseTableCatalogName',baseTableCatalogName) ('referencedTableSchemaName',referencedTableSchemaName) ('referencedTableCatalogName',referencedTableCatalogName) ('deferrable',deferrable) ('initiallyDeferred',initiallyDeferred) ('deleteCascade',deleteCascade) ('onDelete',onDelete) ('onUpdate',onUpdate) ('referencesUniqueColumn',referencesUniqueColumn) ('validate',validate).asMap
	}

	/**  */
	void addForeignKeyConstraint( Map<String, Object> namedArgs, String baseTableName, String baseColumnNames, String constraintName, String referencedTableName, String referencedColumnNames, String baseTableSchemaName=null, String baseTableCatalogName=null, String referencedTableSchemaName=null, String referencedTableCatalogName=null, Boolean deferrable=null, Boolean initiallyDeferred=null, Boolean deleteCascade=null, FkCascadeActionOptions onDelete=null, FkCascadeActionOptions onUpdate=null, Boolean referencesUniqueColumn=null, Boolean validate=null) {
		addForeignKeyConstraint chkMap(Tag.addForeignKeyConstraint,namedArgs) ('baseTableName',baseTableName) ('baseColumnNames',baseColumnNames) ('constraintName',constraintName) ('referencedTableName',referencedTableName) ('referencedColumnNames',referencedColumnNames) ('baseTableSchemaName',baseTableSchemaName) ('baseTableCatalogName',baseTableCatalogName) ('referencedTableSchemaName',referencedTableSchemaName) ('referencedTableCatalogName',referencedTableCatalogName) ('deferrable',deferrable) ('initiallyDeferred',initiallyDeferred) ('deleteCascade',deleteCascade) ('onDelete',onDelete) ('onUpdate',onUpdate) ('referencesUniqueColumn',referencesUniqueColumn) ('validate',validate).asMap
	}

	/**  */
	void addForeignKeyConstraint(Map<String, Object> params) {
		addChange Tag.addForeignKeyConstraint, params
	}

	/**  */
	void dropForeignKeyConstraint( String constraintName, String baseTableName, String baseTableSchemaName=null, String baseTableCatalogName=null) {
		dropForeignKeyConstraint chkMap(Tag.dropForeignKeyConstraint) ('constraintName',constraintName) ('baseTableName',baseTableName) ('baseTableSchemaName',baseTableSchemaName) ('baseTableCatalogName',baseTableCatalogName).asMap
	}

	/**  */
	void dropForeignKeyConstraint( Map<String, Object> namedArgs, String constraintName, String baseTableName, String baseTableSchemaName=null, String baseTableCatalogName=null) {
		dropForeignKeyConstraint chkMap(Tag.dropForeignKeyConstraint,namedArgs) ('constraintName',constraintName) ('baseTableName',baseTableName) ('baseTableSchemaName',baseTableSchemaName) ('baseTableCatalogName',baseTableCatalogName).asMap
	}

	/**  */
	void dropForeignKeyConstraint(Map<String, Object> params) {
		addChange Tag.dropForeignKeyConstraint, params
	}

	/**  */
	void dropAllForeignKeyConstraints( String baseTableName, String baseTableSchemaName=null, String baseTableCatalogName=null) {
		dropAllForeignKeyConstraints chkMap(Tag.dropAllForeignKeyConstraints) ('baseTableName',baseTableName) ('baseTableSchemaName',baseTableSchemaName) ('baseTableCatalogName',baseTableCatalogName).asMap
	}

	/**  */
	void dropAllForeignKeyConstraints( Map<String, Object> namedArgs, String baseTableName, String baseTableSchemaName=null, String baseTableCatalogName=null) {
		dropAllForeignKeyConstraints chkMap(Tag.dropAllForeignKeyConstraints,namedArgs) ('baseTableName',baseTableName) ('baseTableSchemaName',baseTableSchemaName) ('baseTableCatalogName',baseTableCatalogName).asMap
	}

	/**  */
	void dropAllForeignKeyConstraints(Map<String, Object> params) {
		addChange Tag.dropAllForeignKeyConstraints, params
	}

	/**  */
	void addPrimaryKey( String columnNames, String tableName, String schemaName=null, String catalogName=null, String constraintName=null, String tablespace=null, Boolean clustered=null, String forIndexName=null, String forIndexSchemaName=null, String forIndexCatalogName=null, Boolean validate=null) {
		addPrimaryKey chkMap(Tag.addPrimaryKey) ('columnNames',columnNames) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('constraintName',constraintName) ('tablespace',tablespace) ('clustered',clustered) ('forIndexName',forIndexName) ('forIndexSchemaName',forIndexSchemaName) ('forIndexCatalogName',forIndexCatalogName) ('validate',validate).asMap
	}

	/**  */
	void addPrimaryKey( Map<String, Object> namedArgs, String columnNames, String tableName, String schemaName=null, String catalogName=null, String constraintName=null, String tablespace=null, Boolean clustered=null, String forIndexName=null, String forIndexSchemaName=null, String forIndexCatalogName=null, Boolean validate=null) {
		addPrimaryKey chkMap(Tag.addPrimaryKey,namedArgs) ('columnNames',columnNames) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('constraintName',constraintName) ('tablespace',tablespace) ('clustered',clustered) ('forIndexName',forIndexName) ('forIndexSchemaName',forIndexSchemaName) ('forIndexCatalogName',forIndexCatalogName) ('validate',validate).asMap
	}

	/**  */
	void addPrimaryKey(Map<String, Object> params) {
		addChange Tag.addPrimaryKey, params
	}

	/**  */
	void dropPrimaryKey( String tableName, String constraintName=null, String schemaName=null, String catalogName=null, Boolean dropIndex=null) {
		dropPrimaryKey chkMap(Tag.dropPrimaryKey) ('tableName',tableName) ('constraintName',constraintName) ('schemaName',schemaName) ('catalogName',catalogName) ('dropIndex',dropIndex).asMap
	}

	/**  */
	void dropPrimaryKey( Map<String, Object> namedArgs, String tableName, String constraintName=null, String schemaName=null, String catalogName=null, Boolean dropIndex=null) {
		dropPrimaryKey chkMap(Tag.dropPrimaryKey,namedArgs) ('tableName',tableName) ('constraintName',constraintName) ('schemaName',schemaName) ('catalogName',catalogName) ('dropIndex',dropIndex).asMap
	}

	/**  */
	void dropPrimaryKey(Map<String, Object> params) {
		addChange Tag.dropPrimaryKey, params
	}

	/**  */
	void addLookupTable( String existingTableName, String existingColumnName, String newTableName, String newColumnName, String existingTableCatalogName=null, String existingTableSchemaName=null, String newTableCatalogName=null, String newTableSchemaName=null, String newColumnDataType=null, String constraintName=null) {
		addLookupTable chkMap(Tag.addLookupTable) ('existingTableName',existingTableName) ('existingColumnName',existingColumnName) ('newTableName',newTableName) ('newColumnName',newColumnName) ('existingTableCatalogName',existingTableCatalogName) ('existingTableSchemaName',existingTableSchemaName) ('newTableCatalogName',newTableCatalogName) ('newTableSchemaName',newTableSchemaName) ('newColumnDataType',newColumnDataType) ('constraintName',constraintName).asMap
	}

	/**  */
	void addLookupTable( Map<String, Object> namedArgs, String existingTableName, String existingColumnName, String newTableName, String newColumnName, String existingTableCatalogName=null, String existingTableSchemaName=null, String newTableCatalogName=null, String newTableSchemaName=null, String newColumnDataType=null, String constraintName=null) {
		addLookupTable chkMap(Tag.addLookupTable,namedArgs) ('existingTableName',existingTableName) ('existingColumnName',existingColumnName) ('newTableName',newTableName) ('newColumnName',newColumnName) ('existingTableCatalogName',existingTableCatalogName) ('existingTableSchemaName',existingTableSchemaName) ('newTableCatalogName',newTableCatalogName) ('newTableSchemaName',newTableSchemaName) ('newColumnDataType',newColumnDataType) ('constraintName',constraintName).asMap
	}

	/**  */
	void addLookupTable(Map<String, Object> params) {
		addChange Tag.addLookupTable, params
	}

	/**  */
	void addAutoIncrement( String columnName, String tableName, String columnDataType=null, Long startWith=null, Long incrementBy=null, Boolean defaultOnNull=null, String generationType=null, String schemaName=null, String catalogName=null) {
		addAutoIncrement chkMap(Tag.addAutoIncrement) ('columnName',columnName) ('tableName',tableName) ('columnDataType',columnDataType) ('startWith',startWith) ('incrementBy',incrementBy) ('defaultOnNull',defaultOnNull) ('generationType',generationType) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void addAutoIncrement( Map<String, Object> namedArgs, String columnName, String tableName, String columnDataType=null, Long startWith=null, Long incrementBy=null, Boolean defaultOnNull=null, String generationType=null, String schemaName=null, String catalogName=null) {
		addAutoIncrement chkMap(Tag.addAutoIncrement,namedArgs) ('columnName',columnName) ('tableName',tableName) ('columnDataType',columnDataType) ('startWith',startWith) ('incrementBy',incrementBy) ('defaultOnNull',defaultOnNull) ('generationType',generationType) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void addAutoIncrement(Map<String, Object> params) {
		addChange Tag.addAutoIncrement, params
	}

	/**  */
	void addDefaultValue( String columnName, String tableName, String defaultValue=null, String defaultValueNumeric=null, String defaultValueDate=null, String defaultValueBoolean=null, String defaultValueComputed=null, String defaultValueSequenceNext=null, String defaultValueConstraintName=null, String columnDataType=null, String schemaName=null, String catalogName=null) {
		addDefaultValue chkMap(Tag.addDefaultValue) ('columnName',columnName) ('tableName',tableName) ('defaultValue',defaultValue) ('defaultValueNumeric',defaultValueNumeric) ('defaultValueDate',defaultValueDate) ('defaultValueBoolean',defaultValueBoolean) ('defaultValueComputed',defaultValueComputed) ('defaultValueSequenceNext',defaultValueSequenceNext) ('defaultValueConstraintName',defaultValueConstraintName) ('columnDataType',columnDataType) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void addDefaultValue( Map<String, Object> namedArgs, String columnName, String tableName, String defaultValue=null, String defaultValueNumeric=null, String defaultValueDate=null, String defaultValueBoolean=null, String defaultValueComputed=null, String defaultValueSequenceNext=null, String defaultValueConstraintName=null, String columnDataType=null, String schemaName=null, String catalogName=null) {
		addDefaultValue chkMap(Tag.addDefaultValue,namedArgs) ('columnName',columnName) ('tableName',tableName) ('defaultValue',defaultValue) ('defaultValueNumeric',defaultValueNumeric) ('defaultValueDate',defaultValueDate) ('defaultValueBoolean',defaultValueBoolean) ('defaultValueComputed',defaultValueComputed) ('defaultValueSequenceNext',defaultValueSequenceNext) ('defaultValueConstraintName',defaultValueConstraintName) ('columnDataType',columnDataType) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void addDefaultValue(Map<String, Object> params) {
		addChange Tag.addDefaultValue, params
	}

	/**  */
	void dropDefaultValue( String columnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null) {
		dropDefaultValue chkMap(Tag.dropDefaultValue) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('columnDataType',columnDataType).asMap
	}

	/**  */
	void dropDefaultValue( Map<String, Object> namedArgs, String columnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null) {
		dropDefaultValue chkMap(Tag.dropDefaultValue,namedArgs) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('columnDataType',columnDataType).asMap
	}

	/**  */
	void dropDefaultValue(Map<String, Object> params) {
		addChange Tag.dropDefaultValue, params
	}

	/**  */
	void addUniqueConstraint( String columnNames, String tableName, String schemaName=null, String catalogName=null, String constraintName=null, String tablespace=null, Boolean disabled=null, Boolean deferrable=null, Boolean initiallyDeferred=null, String forIndexCatalogName=null, String forIndexSchemaName=null, String forIndexName=null, Boolean clustered=null, Boolean validate=null) {
		addUniqueConstraint chkMap(Tag.addUniqueConstraint) ('columnNames',columnNames) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('constraintName',constraintName) ('tablespace',tablespace) ('disabled',disabled) ('deferrable',deferrable) ('initiallyDeferred',initiallyDeferred) ('forIndexCatalogName',forIndexCatalogName) ('forIndexSchemaName',forIndexSchemaName) ('forIndexName',forIndexName) ('clustered',clustered) ('validate',validate).asMap
	}

	/**  */
	void addUniqueConstraint( Map<String, Object> namedArgs, String columnNames, String tableName, String schemaName=null, String catalogName=null, String constraintName=null, String tablespace=null, Boolean disabled=null, Boolean deferrable=null, Boolean initiallyDeferred=null, String forIndexCatalogName=null, String forIndexSchemaName=null, String forIndexName=null, Boolean clustered=null, Boolean validate=null) {
		addUniqueConstraint chkMap(Tag.addUniqueConstraint,namedArgs) ('columnNames',columnNames) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('constraintName',constraintName) ('tablespace',tablespace) ('disabled',disabled) ('deferrable',deferrable) ('initiallyDeferred',initiallyDeferred) ('forIndexCatalogName',forIndexCatalogName) ('forIndexSchemaName',forIndexSchemaName) ('forIndexName',forIndexName) ('clustered',clustered) ('validate',validate).asMap
	}

	/**  */
	void addUniqueConstraint(Map<String, Object> params) {
		addChange Tag.addUniqueConstraint, params
	}

	/**  */
	void dropUniqueConstraint( String constraintName, String tableName, String schemaName=null, String catalogName=null, String uniqueColumns=null) {
		dropUniqueConstraint chkMap(Tag.dropUniqueConstraint) ('constraintName',constraintName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('uniqueColumns',uniqueColumns).asMap
	}

	/**  */
	void dropUniqueConstraint( Map<String, Object> namedArgs, String constraintName, String tableName, String schemaName=null, String catalogName=null, String uniqueColumns=null) {
		dropUniqueConstraint chkMap(Tag.dropUniqueConstraint,namedArgs) ('constraintName',constraintName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('uniqueColumns',uniqueColumns).asMap
	}

	/**  */
	void dropUniqueConstraint(Map<String, Object> params) {
		addChange Tag.dropUniqueConstraint, params
	}

	/**  */
	void setTableRemarks( String remarks, String tableName, String schemaName=null, String catalogName=null) {
		setTableRemarks chkMap(Tag.setTableRemarks) ('remarks',remarks) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void setTableRemarks( Map<String, Object> namedArgs, String remarks, String tableName, String schemaName=null, String catalogName=null) {
		setTableRemarks chkMap(Tag.setTableRemarks,namedArgs) ('remarks',remarks) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void setTableRemarks(Map<String, Object> params) {
		addChange Tag.setTableRemarks, params
	}

	/**  */
	void setColumnRemarks( String remarks, String columnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null, ColumnParentTypeEnum columnParentType=null) {
		setColumnRemarks chkMap(Tag.setColumnRemarks) ('remarks',remarks) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('columnDataType',columnDataType) ('columnParentType',columnParentType).asMap
	}

	/**  */
	void setColumnRemarks( Map<String, Object> namedArgs, String remarks, String columnName, String tableName, String schemaName=null, String catalogName=null, String columnDataType=null, ColumnParentTypeEnum columnParentType=null) {
		setColumnRemarks chkMap(Tag.setColumnRemarks,namedArgs) ('remarks',remarks) ('columnName',columnName) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) ('columnDataType',columnDataType) ('columnParentType',columnParentType).asMap
	}

	/**  */
	void setColumnRemarks(Map<String, Object> params) {
		addChange Tag.setColumnRemarks, params
	}

	/** Update data in the specified table */
	void update( String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=UpdateDelegate, strategy=DELEGATE_ONLY) Closure children) {
		update chkMap(Tag.update) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, children
	}

	/** Update data in the specified table */
	void update( Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=UpdateDelegate, strategy=DELEGATE_ONLY) Closure children) {
		update chkMap(Tag.update,namedArgs) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, children
	}

	/** Update data in the specified table */
	void update(Map<String, Object> params, 
				@DelegatesTo(value=UpdateDelegate, strategy=DELEGATE_ONLY) Closure children) {
		addChangeWithChild Tag.update, params, children
	}

	/**  */
	void delete( String tableName, String schemaName=null, String catalogName=null) {
		delete chkMap(Tag.delete) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void delete( Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null) {
		delete chkMap(Tag.delete,namedArgs) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName).asMap
	}

	/**  */
	void delete( String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=DeleteDelegate, strategy=DELEGATE_ONLY) Closure children) {
		delete chkMap(Tag.delete) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, children
	}

	/**  */
	void delete( Map<String, Object> namedArgs, String tableName, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=DeleteDelegate, strategy=DELEGATE_ONLY) Closure children) {
		delete chkMap(Tag.delete,namedArgs) ('tableName',tableName) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, children
	}

	/**  */
	void delete(Map<String, Object> params, 
				@DelegatesTo(value=DeleteDelegate, strategy=DELEGATE_ONLY) Closure children=null) {
		addChangeWithChild Tag.delete, params, children
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
		loadData chkMap(Tag.loadData) ('tableName',tableName) ('file',file) ('relativeToChangelogFile',relativeToChangelogFile) ('encoding',encoding) ('separator',separator) ('quotchar',quotchar) ('commentLineStartsWith',commentLineStartsWith) ('usePreparedStatements',usePreparedStatements) ('schemaName',schemaName) ('catalogName',catalogName).asMap
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
	void loadData( Map<String, Object> namedArgs, String tableName, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null) {
		loadData chkMap(Tag.loadData,namedArgs) ('tableName',tableName) ('file',file) ('relativeToChangelogFile',relativeToChangelogFile) ('encoding',encoding) ('separator',separator) ('quotchar',quotchar) ('commentLineStartsWith',commentLineStartsWith) ('usePreparedStatements',usePreparedStatements) ('schemaName',schemaName) ('catalogName',catalogName).asMap
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
		loadData chkMap(Tag.loadData) ('tableName',tableName) ('file',file) ('relativeToChangelogFile',relativeToChangelogFile) ('encoding',encoding) ('separator',separator) ('quotchar',quotchar) ('commentLineStartsWith',commentLineStartsWith) ('usePreparedStatements',usePreparedStatements) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, columns
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
	void loadData( Map<String, Object> namedArgs, String tableName, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null, 
				@DelegatesTo(value=LoadDataDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		loadData chkMap(Tag.loadData,namedArgs) ('tableName',tableName) ('file',file) ('relativeToChangelogFile',relativeToChangelogFile) ('encoding',encoding) ('separator',separator) ('quotchar',quotchar) ('commentLineStartsWith',commentLineStartsWith) ('usePreparedStatements',usePreparedStatements) ('schemaName',schemaName) ('catalogName',catalogName) .asMap, columns
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
		loadUpdateData chkMap(Tag.loadUpdateData) ('tableName',tableName) ('primaryKey',primaryKey) ('file',file) ('relativeToChangelogFile',relativeToChangelogFile) ('encoding',encoding) ('separator',separator) ('quotchar',quotchar) ('commentLineStartsWith',commentLineStartsWith) ('usePreparedStatements',usePreparedStatements) ('schemaName',schemaName) ('catalogName',catalogName) ('onlyUpdate',onlyUpdate).asMap
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
	void loadUpdateData( Map<String, Object> namedArgs, String tableName, String primaryKey, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null, Boolean onlyUpdate=null) {
		loadUpdateData chkMap(Tag.loadUpdateData,namedArgs) ('tableName',tableName) ('primaryKey',primaryKey) ('file',file) ('relativeToChangelogFile',relativeToChangelogFile) ('encoding',encoding) ('separator',separator) ('quotchar',quotchar) ('commentLineStartsWith',commentLineStartsWith) ('usePreparedStatements',usePreparedStatements) ('schemaName',schemaName) ('catalogName',catalogName) ('onlyUpdate',onlyUpdate).asMap
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
				@DelegatesTo(value=LoadUpdateDataDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		loadUpdateData chkMap(Tag.loadUpdateData) ('tableName',tableName) ('primaryKey',primaryKey) ('file',file) ('relativeToChangelogFile',relativeToChangelogFile) ('encoding',encoding) ('separator',separator) ('quotchar',quotchar) ('commentLineStartsWith',commentLineStartsWith) ('usePreparedStatements',usePreparedStatements) ('schemaName',schemaName) ('catalogName',catalogName) ('onlyUpdate',onlyUpdate) .asMap, columns
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
	void loadUpdateData( Map<String, Object> namedArgs, String tableName, String primaryKey, String file, Boolean relativeToChangelogFile=null, String encoding=null, String separator=null, String quotchar=null, String commentLineStartsWith=null, Boolean usePreparedStatements=null, String schemaName=null, String catalogName=null, Boolean onlyUpdate=null, 
				@DelegatesTo(value=LoadUpdateDataDelegate, strategy=DELEGATE_ONLY) Closure columns) {
		loadUpdateData chkMap(Tag.loadUpdateData,namedArgs) ('tableName',tableName) ('primaryKey',primaryKey) ('file',file) ('relativeToChangelogFile',relativeToChangelogFile) ('encoding',encoding) ('separator',separator) ('quotchar',quotchar) ('commentLineStartsWith',commentLineStartsWith) ('usePreparedStatements',usePreparedStatements) ('schemaName',schemaName) ('catalogName',catalogName) ('onlyUpdate',onlyUpdate) .asMap, columns
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
				@DelegatesTo(value=LoadUpdateDataDelegate, strategy=DELEGATE_ONLY) Closure columns=null) {
		addChangeWithChild Tag.loadUpdateData, params, columns
	}

	/** Execute a shell command. */
	void executeCommand( String executable, String os=null, String timeout=null) {
		executeCommand chkMap(Tag.executeCommand) ('executable',executable) ('os',os) ('timeout',timeout).asMap
	}

	/** Execute a shell command. */
	void executeCommand( Map<String, Object> namedArgs, String executable, String os=null, String timeout=null) {
		executeCommand chkMap(Tag.executeCommand,namedArgs) ('executable',executable) ('os',os) ('timeout',timeout).asMap
	}

	/** Execute a shell command. */
	void executeCommand( String executable, String os=null, String timeout=null, 
				@DelegatesTo(value=ExecuteCommandDelegate, strategy=DELEGATE_ONLY) Closure args) {
		executeCommand chkMap(Tag.executeCommand) ('executable',executable) ('os',os) ('timeout',timeout) .asMap, args
	}

	/** Execute a shell command. */
	void executeCommand( Map<String, Object> namedArgs, String executable, String os=null, String timeout=null, 
				@DelegatesTo(value=ExecuteCommandDelegate, strategy=DELEGATE_ONLY) Closure args) {
		executeCommand chkMap(Tag.executeCommand,namedArgs) ('executable',executable) ('os',os) ('timeout',timeout) .asMap, args
	}

	/** Execute a shell command. */
	void executeCommand(Map<String, Object> params, 
				@DelegatesTo(value=ExecuteCommandDelegate, strategy=DELEGATE_ONLY) Closure args=null) {
		addChangeWithChild Tag.executeCommand, params, args
	}

}
