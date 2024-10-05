package org.liquibase.groovy.delegate

import liquibase.changelog.DatabaseChangeLog
import static groovy.lang.Closure.*

@groovy.transform.CompileStatic
/** Class for generic functions in ...Delegate classes */
abstract class Delegatee {
    protected final DatabaseChangeLog databaseChangeLog
    protected String changeSetId // used for error messages
    Delegatee(DatabaseChangeLog dbChangeLog, String changeSetId = null){
        databaseChangeLog = dbChangeLog
        this.changeSetId = changeSetId
    }

    /** call the given closure with this as delegate */
    def call(@DelegatesTo(strategy = DELEGATE_ONLY) Closure closure) {
        closure.delegate = this
        closure.resolveStrategy = DELEGATE_FIRST
        closure.call()
    }
}
