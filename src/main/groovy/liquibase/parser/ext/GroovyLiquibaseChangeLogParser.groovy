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

package liquibase.parser.ext

import liquibase.parser.ChangeLogParser
import liquibase.changelog.DatabaseChangeLog
import liquibase.changelog.ChangeLogParameters
import liquibase.resource.ResourceAccessor
import liquibase.exception.ChangeLogParseException
import org.codehaus.groovy.control.CompilationFailedException
import org.codehaus.groovy.control.CompilerConfiguration
import org.codehaus.groovy.control.customizers.ImportCustomizer
import org.codehaus.groovy.runtime.metaclass.MethodSelectionException
import org.liquibase.groovy.delegate.DatabaseChangeLogDelegate

/**
 * This is the main parser class for the Liquibase Groovy DSL.  It is the integration point to
 * Liquibase itself.  It must be in the liquibase.parser.ext package to be found by Liquibase at
 * runtime.
 *
 * @author Tim Berglund
 * @author Steven C. Saliman
 */
class GroovyLiquibaseChangeLogParser implements ChangeLogParser {

    DatabaseChangeLog parse(String physicalChangeLogLocation,
                            ChangeLogParameters changeLogParameters,
                            ResourceAccessor resourceAccessor) {

        physicalChangeLogLocation = physicalChangeLogLocation.replaceAll('\\\\', '/')
        def inputStream = resourceAccessor.openStream(null, physicalChangeLogLocation)
        if ( !inputStream ) {
            throw new ChangeLogParseException(physicalChangeLogLocation + " does not exist")
        }
        parse(inputStream, resourceAccessor, changeLogParameters, physicalChangeLogLocation)
    }

    DatabaseChangeLog parse(InputStream inputStream, ResourceAccessor resourceAccessor,
                                   ChangeLogParameters changeLogParameters = new ChangeLogParameters(),
                                   String physicalChangeLogLocation = 'memtest'
                            ) {
        try {
            DatabaseChangeLog changeLog = new DatabaseChangeLog(physicalChangeLogLocation)
            changeLog.setChangeLogParameters(changeLogParameters)

            def binding = new Binding()
            def config = new CompilerConfiguration()
            config.scriptBaseClass = 'liquibase.parser.ext.ParserScript'
            config.addCompilationCustomizers(new ImportCustomizer().
                    addStaticStars('liquibase.database.ObjectQuotingStrategy')
            )
            def shell = new GroovyShell(binding, config)

            // Parse the script, give it the local changeLog instance, give it access to root-level
            // method delegates, and call.
            Script script = shell.parse(new InputStreamReader(inputStream, "UTF8")
                                            ,physicalChangeLogLocation)
            script.setProperty("changeLog", changeLog)
            script.setProperty("resourceAccessor", resourceAccessor)
            try {
                script.run()
            } catch(MethodSelectionException e) {
                String methodName = e.metaClass.getAttribute(e, 'methodName')
                Class[] argTypes = e.metaClass.getAttribute(e, 'arguments') as Class[]
                def methods = e.metaClass.getAttribute(e, 'methods')
                // All method must have closure as last if required
                MetaMethod method = script.metaClass.methods.find {it.name == methodName}
                println script.class.canonicalName
                // Most common problem: closure missing as last parameter
                if( method.nativeParameterTypes.last() == Closure.class &&
                        (argTypes.length < 1 || argTypes.last() != Closure.class)) {
                    throw ChangeLogParseExceptionWithfileAndLineNumber(changeLog, e, script.class,
                            elemMissingRequiredClosure(methodName))
                }
                 throw e
            }
            // The changeLog will have been populated by the script
            return changeLog
        }
        finally {
            try {
                inputStream.close()
            }
            catch (Exception ignored) {
                // Can't do much more than hope for the best here
            }
        }
    }


    boolean supports(String changeLogFile, ResourceAccessor resourceAccessor) {
        changeLogFile.endsWith('.groovy')
    }


    int getPriority() {
        PRIORITY_DEFAULT
    }

    static String elemMissingRequiredClosure(String name) {"'$name' missing required closure"}
    static final String databaseChangeMissingClosure = "databaseChangeLog element missing required closure!" + elementWithClosure
    static String databaseChangeLogInvalidArgs(Object[] args) { "databaseChangeLog element got invalid arguments ${argsToString(args)}" + elementWithClosure }
    static final String elementWithClosure = "\nIt can take optional parameters followed by a required closure: 'databaseChangeLog { ... }' or 'databaseChangeLog(param1,...) { ... }'"

    static String argsToString(Object[] args){
        args.inject(""){ String acc, val ->
            if(!acc.empty){
                acc += ','
            }
            String v
            if(val instanceof Closure) v = '{}'
            else v = val.toString()
            acc + v
        }
    }

    /** Create a new ChangeLogParseException with the message `errMsg` if not null
        otherwise t.message + the filename from `databaseChangeLog` and the line number from
        the stacktrace of `t` searching for the classname of `clazz`
        If errMsg is null, t added to the created exception as cause
     */
    static ChangeLogParseException ChangeLogParseExceptionWithfileAndLineNumber(
            DatabaseChangeLog databaseChangeLog, Throwable t, Class calzz, String errMsg = null) {
        boolean bAddException = null == errMsg
        StackTraceElement st = t.stackTrace.find { it.getClassName() == calzz.name }
        if(!errMsg) errMsg = t.message
        if ( st ) {
            errMsg += ' @'+ databaseChangeLog.physicalFilePath + ":" + st.lineNumber
        }
        bAddException ? new ChangeLogParseException(errMsg, t) : new ChangeLogParseException(errMsg)
    }

    static void processDatabaseChangeLogRootElement(DatabaseChangeLog databaseChangeLog,
                                                   ResourceAccessor resourceAccessor, Object[] args) {
        DatabaseChangeLogDelegate delegate
        Closure closure

        switch ( args.size() ) {
            case 0:
                throw new ChangeLogParseException(databaseChangeMissingClosure)

            case 1:
                if ( !(args[0] instanceof Closure) ) {
                    throw new ChangeLogParseException(databaseChangeLogInvalidArgs(args))
                }
                closure = args[0] as Closure
                delegate = new DatabaseChangeLogDelegate(databaseChangeLog)
                break

            case 2:
                if ( !(args[0] instanceof Map) || !(args[1] instanceof Closure)  ) {
                    throw new ChangeLogParseException(databaseChangeLogInvalidArgs(args))
                }
                Map params = args[0] as Map
                closure = args[1] as Closure
                delegate = new DatabaseChangeLogDelegate(params, databaseChangeLog)
                break

            default:
                throw new ChangeLogParseException("databaseChangeLog element has too many arguments: ${args}")
        }

        delegate.resourceAccessor = resourceAccessor
        closure.delegate = delegate
        closure.resolveStrategy = Closure.DELEGATE_FIRST
        try {
            closure.call()
        }
        catch (CompilationFailedException e){
            throw e
        }
        catch (e) {
            throw ChangeLogParseExceptionWithfileAndLineNumber(databaseChangeLog, e, closure.class)
        }
    }
}

