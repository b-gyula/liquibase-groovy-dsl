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

import liquibase.changelog.DatabaseChangeLog
import liquibase.database.ObjectQuotingStrategy
import liquibase.exception.ChangeLogParseException
import liquibase.resource.ResourceAccessor
import org.liquibase.groovy.delegate.*
import static groovy.lang.Closure.DELEGATE_ONLY
import static GroovyLiquibaseChangeLogParser.processDatabaseChangeLogRootElement

abstract class ParserScript extends Script {

    @Override
    Object invokeMethod(String name, Object args) {
        if ( name == 'databaseChangeLog' ) {
            processDatabaseChangeLogRootElement(getProperty('changeLog') as DatabaseChangeLog,
                    getProperty('resourceAccessor') as ResourceAccessor, args)
        } else {
            throw new ChangeLogParseException("Unrecognized root element ${name}")
        }
    }

    /** Root element of the changelog
     <br>Params:
     <dl>
     <dt>contextFilter</dt>
     <dd>The filter (coma separated list) defining which contexts are required the databaseChangeLog to process.
     <a href="https://docs.liquibase.com/concepts/changelogs/attributes/contexts.html">Contexts</a>
     are tags you can add to changesets to control which changesets are executed in any particular migration run.
     Contexts you specify in the databaseChangeLog header are inherited by individual changeSets.
     renamed from "context" since v4.16</dd>
     <dt>logicalFilePath</dt>
     <dd>Overrides the file name and path when creating the unique identifier of changesets.
     It is required when you want to move or rename changelogs. Default: physical path</dd>
     <dt>objectQuotingStrategy</dt>
     <dd>the SQL object quoting strategy defaults to LEGACY</dd>
     </dl>
     @see liquibase.database.ObjectQuotingStrategy
     */
    void databaseChangeLog( Map<String, Object> args
                           ,String logicalFilePath = null, String contextFilter = null // These are required for mixed parameter calls
                           ,ObjectQuotingStrategy objectQuotingStrategy = null
                           ,@DelegatesTo(value = DatabaseChangeLogDelegate, strategy = DELEGATE_ONLY) Closure closure) {
        // It is required to accept all versions containing named parameter must remain for backward compatibility!!!
        args.putAll(
                [logicalFilePath      : logicalFilePath,
                 contextFilter        : contextFilter,
                 objectQuotingStrategy: objectQuotingStrategy
                ].findAll { it.value != null })
        invokeMethod ('databaseChangeLog', new Object[]{args, closure})
    }

    /** Root element of the changelog
     <br>Params:
     <dl>
     <dt>contextFilter</dt>
     <dd>The filter (coma separated list) defining which contexts are required the databaseChangeLog to process.
     <a href="https://docs.liquibase.com/concepts/changelogs/attributes/contexts.html">Contexts</a> are tags you can add to changesets to control which changesets are executed in any particular migration run.
     Contexts you specify in the databaseChangeLog header are inherited by individual changeSets.
     renamed from "context" since v4.16</dd>
     <dt>logicalFilePath</dt>
     <dd>Overrides the file name and path when creating the unique identifier of changesets.
     It is required when you want to move or rename changelogs. Default: physical path</dd>
     <dt>objectQuotingStrategy</dt>
     <dd>the SQL object quoting strategy defaults to LEGACY</dd>
     </dl>
     @see liquibase.database.ObjectQuotingStrategy
     */
    void databaseChangeLog(String logicalFilePath = null, String contextFilter = null,
                           ObjectQuotingStrategy objectQuotingStrategy = null,
                           @DelegatesTo(value = DatabaseChangeLogDelegate, strategy = DELEGATE_ONLY) Closure closure) {
         databaseChangeLog [:], logicalFilePath, contextFilter, objectQuotingStrategy, closure
    }

    /**  */
    def propertyMissing(String name)  { // Pure "databaseChangeLog" / "nonExistentName"
        invokeMethod(name, new Object[]{}) // TODO add filename an linenumber
    }
}
