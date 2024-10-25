/*
 * Copyright 2011-2024 Tim Berglund and Steven C. Saliman
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.liquibase.groovy.delegate

import groovy.transform.TypeChecked
import groovy.transform.TypeCheckingMode
import liquibase.changelog.DatabaseChangeLog
import liquibase.exception.ChangeLogParseException
import liquibase.parser.groovy.exception.ParseErrorWithFileNLine
import liquibase.precondition.Precondition
import liquibase.precondition.PreconditionLogic
import liquibase.precondition.core.AndPrecondition
import liquibase.precondition.core.OrPrecondition
import liquibase.precondition.core.SqlPrecondition
import liquibase.precondition.CustomPreconditionWrapper
import liquibase.precondition.PreconditionFactory
import liquibase.precondition.core.NotPrecondition
import liquibase.precondition.core.PreconditionContainer
import liquibase.precondition.core.PreconditionContainer.OnSqlOutputOption
import liquibase.precondition.core.PreconditionContainer.ErrorOption
import liquibase.precondition.core.PreconditionContainer.FailOption
import liquibase.util.PatchedObjectUtil

import java.lang.reflect.Method

import static groovy.lang.Closure.DELEGATE_FIRST
import static liquibase.parser.ext.GroovyLiquibaseChangeLogParser.dbChangeLogTagName
import static liquibase.parser.groovy.exception.InvalidArgument.argsToString
import static org.liquibase.groovy.delegate.PreconditionDelegate.Tag.indexExists
import static org.liquibase.groovy.delegate.PreconditionDelegate.Tag.indexExists

@groovy.transform.CompileStatic
/** Delegate for the preConditions element used both in changeSet and databaseChangeLog */
class PreconditionDelegate extends Delegatee<Tag> implements PreConditionChildren {
    static enum Tag { changeLogPropertyDefined, changeSetExecuted, columnExists, dbms,
                      expectedQuotingStrategy,  foreignKeyConstraintExists, indexExists,
                      primaryKeyExists, rowCount, runningAs, sequenceExists, tableExists,
                      tableIsEmpty, uniqueConstraintExists, viewExists }

    protected final List<Precondition> preconditions = []

    PreconditionDelegate(DatabaseChangeLog dbChangeLog, String changeId){
        super(dbChangeLog, changeId + '/preConditions')
    }

    protected void propertyMissing(String name) {
        methodMissing name, null
    }

    protected void methodMissing(String name, args) {
        addPrecondition name, args as Object[]
    }

    protected void addPrecondition(Tag name, Object... args ) {
        addPrecondition name.name(), args
    }

    /**
     * Handle all non-nesting preconditions using the PreconditionFactory.
     * @param name the name of the precondition to create
     * @param args the attributes of the new precondition
     */
    protected void addPrecondition(String name, Object... args ) {
        def preconditionFactory = PreconditionFactory.instance
        Precondition precondition = null
        try {
            precondition = preconditionFactory.create(name)
        } catch (RuntimeException e) {
            throw changeLogParseException("'${name}' is an invalid precondition.", e)
        }

        // We don't always get an exception for an invalid precondition...
        if ( precondition == null ) {
            throw changeLogParseException("'${name}' is an invalid precondition.")
        }

        Method m = methodDefs[name]
        if(m) { // There is a dedicated method
            setProps precondition, argsAsMap(m, args)
        }
        else if ( args != null && args[0] instanceof Map<String, Object> ) {
            setProps(precondition, args[0] as Map<String, Object>)
        } else {
            logWarning("Unable to handle arguments ${argsToString(args)} for precondition '$name'")
        }

        preconditions << precondition
    }

    protected void setProps(Precondition precondition, Map<String, Object> props) {
        props.each {key, value ->
            setProp(precondition, key, value)
        }
    }

    /** Wrapper for PatchedObjectUtil.setProperty adds detailed error message */
    private void setProp(Precondition precondition, String name, Object value) {
        try {
            if(value != null) {
                PatchedObjectUtil.setProperty(precondition, name,
                        DelegateUtil.expandExpressions(value.toString(), databaseChangeLog))
            }
        } catch (RuntimeException e) {
            throw new ChangeLogParseException("$changeId: '$name' is an invalid property for '${precondition.name}'", e)
        }
    }

    /** Executes an SQL string and checks the returned value. The SQL must return a single row with a single value.
     * @param params the attributes of the precondition
     * @param closure the SQL for the precondition
     * @return the newly created precondition.
     */
    def sqlCheck(Map<String, Object> params = [:],
                 @DelegatesTo(value= SqlPrecondition, strategy=DELEGATE_FIRST) Closure closure) {
        def precondition = new SqlPrecondition()
        setProps precondition, params
        def sql = DelegateUtil.expandExpressions(closure.call(), databaseChangeLog)
        if ( sql != null && sql != "null" ) {
            precondition.sql = sql
        }
        preconditions << precondition
    }

    /**
     * Create a customPrecondition.  A custom precondition is a class that implements the Liquibase
     * customPrecondition.  The code can do anything we want.  Parameters need to be passed to our
     * custom class as key/value pairs, either with the XML style of nested {@code param} blocks,
     * or by calling nested methods where the name of the method becomes the key and the arguments
     * become the value.
     * @param params the params for the precondition, such as the class name.
     * @param closure the closure with nested key/value pairs for the custom precondition.
     */
    def customPrecondition(Map<String, Object> params = [:],
                           @DelegatesTo(value=KeyValueDelegate, strategy = DELEGATE_FIRST) Closure closure) {
        def delegate = new KeyValueDelegate('customPrecondition', changeId)
        delegate.call(closure)

        def precondition = new CustomPreconditionWrapper()
        params.each { key, value ->
            setProp(precondition, key, value)
        }
        delegate.map.each { key, value ->
             // This is a key/value pair in the Liquibase object, so it won't fail.
            def expandedValue = DelegateUtil.expandExpressions(value, databaseChangeLog)
            precondition.setParam(key, expandedValue ? expandedValue : "null" )
        }

        preconditions << precondition
    }

    /** logical AND operator */
    def and(@DelegatesTo(value=PreconditionDelegate, strategy=DELEGATE_FIRST ) Closure closure) {
        preconditions << nestedPrecondition(new AndPrecondition(), closure)
    }

    /** logical OR operator */
    def or(@DelegatesTo(value=PreconditionDelegate, strategy=DELEGATE_FIRST) Closure closure) {
        preconditions << nestedPrecondition(new OrPrecondition(), closure)
    }

    /** logical NOT operator
        For multiple children AND logic is used
     */
    def not(@DelegatesTo(value=PreconditionDelegate, strategy=DELEGATE_FIRST) Closure closure) {
        preconditions << nestedPrecondition(new NotPrecondition(), closure)
    }

    /**
     * execute a {@code preconditions} closure and return the Liquibase
     * {@code PreconditionContainer} it creates.
     * @param databaseChangeLog the database changelog that owns the changesets.
     * @param changeSetId the id of the changeset that owns the precondtions
     * @param params the parameters to the preconditions
     * @param closure nested closures to call.
     * @return the PreconditionContainer it builds.
     */
    @TypeChecked(TypeCheckingMode.SKIP)
    static PreconditionContainer buildPreconditionContainer(DatabaseChangeLog databaseChangeLog,
                                                            Map<String, Object> params,
                        @DelegatesTo(value= PreconditionDelegate, strategy=DELEGATE_FIRST) Closure closure,
                                                            String changeSetId = dbChangeLogTagName) {
        PreconditionContainer preconditions = new PreconditionContainer()

        // Process parameters.  3 of them need a special case.
        params.each {key, value ->
            def paramValue = DelegateUtil.expandExpressions(value, databaseChangeLog)
            if ( key == "onFail" ) {
                preconditions.onFail = FailOption."${paramValue}" // TODO is it required?
            } else if ( key == "onError" ) {
                preconditions.onError = ErrorOption."${paramValue}"
            } else if ( key == "onUpdateSql" || key == "onUpdateSql" ) {
                preconditions.onSqlOutput = OnSqlOutputOption."${paramValue}"
            } else {
                // pass the rest to Liquibase
                try {
                    PatchedObjectUtil.setProperty(preconditions, key, paramValue)
                } catch (RuntimeException e) {
                    throw new ChangeLogParseException("$changeSetId: '${key}' is an invalid property for 'preConditions'", e)
                }
            }
        }

        def delegate = new PreconditionDelegate(databaseChangeLog, changeSetId)
        delegate.nestedPrecondition(preconditions, closure, delegate)
    }


    private <T extends PreconditionLogic> T nestedPrecondition(T nestedPrecondition,
           @DelegatesTo(strategy=DELEGATE_FIRST) Closure closure,
           PreconditionDelegate delegate = new PreconditionDelegate(databaseChangeLog, changeId)) {
        delegate.call(closure)

        delegate.preconditions.each { precondition ->
            nestedPrecondition.addNestedPrecondition(precondition)
        }

        return nestedPrecondition
    }
}

