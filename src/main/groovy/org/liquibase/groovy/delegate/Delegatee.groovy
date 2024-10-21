package org.liquibase.groovy.delegate

import groovy.transform.TupleConstructor
import liquibase.changelog.ChangeSet
import liquibase.changelog.DatabaseChangeLog
import liquibase.exception.ChangeLogParseException

import java.lang.reflect.Method

import static groovy.lang.Closure.*
import static liquibase.parser.ext.GroovyLiquibaseChangeLogParser.*
import static org.liquibase.groovy.delegate.DelegateUtil.MapCategory.ifNotNull
import static liquibase.Scope.getCurrentScope
import liquibase.parser.groovy.exception.*

@groovy.transform.CompileStatic
/** Class for generic functions in ...Delegate classes */
abstract class Delegatee {
    protected final DatabaseChangeLog databaseChangeLog
    String changeId // used for error messages
    /** method cache for error messages */
    protected @Lazy Map<String, Method> methodDefs = getMethods(this.class)
    Set<String> knownElements() {methodDefs.keySet()}
    protected String prefix(String msg) {"$changeId: $msg"}

    Delegatee(DatabaseChangeLog dbChangeLog, String changeId){
        this.databaseChangeLog = dbChangeLog
        this.changeId = changeId
    }

    /** call the given closure with this as delegate */
    def call(@DelegatesTo(strategy = DELEGATE_ONLY) Closure closure, Object... args) {
        closure.delegate = this
        closure.resolveStrategy = DELEGATE_FIRST
        closure.call(args)
    }

    /** Prefix Exception */
    ParseErrorWithFileNLine prefix(ParseErrorWithFileNLine e) {
        e.prefix = changeId
        e
    }

    /** Call this to throw an exception to make sure, the prefix properly is set */
    protected void error(ParseErrorWithFileNLine e) {
        throw prefix(e)
    }

    /** Create generic ChangeLogParseException with the message msg prefixed with the changeId
        For specific cases specific Exception shall be used
     */
    protected ChangeLogParseException changeLogParseException(String msg) {
        new ChangeLogParseException(prefix(msg))
    }

    /** object to allow fluently call {@link #putNotNull} */
    class NullChecker<E extends Enum<E>>{
        final E elem
        public final Map<String, Object> asMap
        NullChecker(E e, Map<String, Object> args) {
            elem = e
            asMap = args
        }

        /** If `value` not null & `key` is not in the map yet put them in the map
            @throws ArgumentSetTwice if key is in the map already
         */
        NullChecker<E> putNotNull( String key, value) throws ArgumentSetTwice {
            ifNotNull(value){
                if(asMap.get(key)){
                    throw new ArgumentSetTwice(elem as String, key, changeId)
                }
                asMap.put(key, it)
            }
            this
        }
    }

    Map<String, Object> argsAsMap(Tag tag, Object... args ) {
        String methodName = tag as String
        Method method = methodDefs[methodName]
        argsToMap changeId, methodName, requiresClosure(method), asString(method.parameters),
                method.parameters*.name, args
    }

    /**
     * @param args expected to get all arguments including the starting Map and closing Closure
     * @param argNames expected to contain all parameter names including also expected Map's name
     */
    static Map<String, Object> argsToMap(String prefix, String methodName, boolean needsClosure,
                                         String fnDef, List<String> argNames, Object... args )
        throws ArgumentSetTwice, InvalidArgument {
        int cl = needsClosure? 1 : 0
        int i = args.first() instanceof Map ? 1 : 0

        // Make sure there are no more ars, than expected
        if(args.length <= argNames.size() ) {
            Map<String, Object> map = i ? args.first() as Map : [:]
            for(int n =1;i < args.length-cl; i++) {
                String name = argNames[n++]
                if(args[i] != null) {
                    if(map[name] != null){
                        throw new ArgumentSetTwice(methodName, name, prefix)
                    }
                    map[name] = args[i]
                }
            }
            return map
        }
        throw new InvalidArgument(methodName, fnDef, prefix, args)
    }

    /** Log a warning prefixed with the change id*/
    void logWarning(String msg) {
        getCurrentScope().getLog(getClass()).warning(prefix(msg));
    }

    static String fullChangeSetId(ChangeSet changeSet){"changeSet ${changeSet.toString(false)}"}

    @TupleConstructor
    static enum ArgDef {
        final Class type
        final boolean required
        final String alias
        final String since
    }

    @TupleConstructor
    static enum MethodDef {
        final ArgDef[] args
    }

}
