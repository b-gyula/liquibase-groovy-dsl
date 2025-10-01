package org.liquibase.groovy.delegate

import groovy.transform.CompileStatic
import groovy.transform.PackageScope
import groovy.transform.TupleConstructor
import liquibase.changelog.ChangeSet
import liquibase.changelog.DatabaseChangeLog
import liquibase.parser.groovy.exception.*
import liquibase.serializer.LiquibaseSerializable
import liquibase.util.PatchedObjectUtil
import org.apache.commons.lang3.StringUtils
import org.liquibase.groovy.delegate.DelegateUtil.CollectionStringBuilder

import java.lang.reflect.Parameter

import static groovy.lang.Closure.DELEGATE_FIRST
import static groovy.lang.Closure.DELEGATE_ONLY
import static liquibase.Scope.getCurrentScope
import static liquibase.parser.ext.GroovyLiquibaseChangeLogParser.getMethods
import static org.liquibase.groovy.delegate.DelegateUtil.MapCategory.ifNotNull
import static org.liquibase.groovy.delegate.DelegateUtil.objArr

@CompileStatic
@groovy.util.logging.Log
/** Class for generic functions in ...Delegate classes */
abstract class Delegatee<Tag extends Enum<Tag>> {
    protected final DatabaseChangeLog databaseChangeLog
    final String changeId // used for error messages
    final String parent

    /** method definition cache used for error messages It contains only the longest argument list with closure
      TODO some tag like property / sql / createView / createProcedure / dropColumn requires 2 MethodDef / method
     */
    protected @Lazy Map<String, MethodDef> methodDefs = methodDefs(this.class)
    MethodDef methodDef(String methodName) {methodDefs[methodName]}
    MethodDef methodDef(Tag methodName) {methodDefs[methodName.name()]}
    protected static Map<String, Map<String, MethodDef>> allMethods = [:]

    static String delegateClassSimpleName(String tagName) {
        StringUtils.capitalize(tagName) + "Delegate"
    }

    /** Get Delegate class for tagname */
    static Class<Delegate> delegateClass4tag(String tagName) {
        (Class<Delegate> )Class.forName("org.liquibase.groovy.delegate." + delegateClassSimpleName(tagName))
    }

    static Map<String, MethodDef> methodDefs4Tag(String tagName) {
         getMethodDefs( delegateClass4tag(tagName ))
    }

    static Map<String, MethodDef> getMethodDefs(Class cls) {
        Map<String, MethodDef> methods = getMethods(cls)
        if(cls.simpleName.contains('Update')) { // Help inheritance
            methods += getMethods(cls.superclass)
        }
        log.fine("Methods for $cls.simpleName: $methods")
        methods
    }

    static Map<String, MethodDef> methodDefs(Class cls) {
        allMethods.computeIfAbsent(cls.simpleName,{ getMethodDefs(cls) })
    }

    Set<String> knownElements() {methodDefs.keySet()}

    protected String prefix(String msg) {"$changeId: $msg"}

    Delegatee(DatabaseChangeLog dbChangeLog, String changeId, String parent = null) {
        this.databaseChangeLog = dbChangeLog
        this.changeId = changeId
        this.parent = parent
    }
/*
    Tag of(String s) {
        try{
            Enum.valueOf(tagClass, s)
        }catch (ignored) {
            null
        }
    }*/

    /** call the given closure with this as delegate */
    def call(@DelegatesTo(strategy = DELEGATE_ONLY) Closure closure, args = null) {
        if(closure) {
            closure.delegate = this
            closure.resolveStrategy = DELEGATE_FIRST
            return closure.call(args)
        }
        null
    }

    /** Prefix Exception */
    ParseErrorWithFileNLine prefix(ParseErrorWithFileNLine e) {
        e.prefix = changeId
        e
    }

    /** Call this to throw an exception to make sure, the prefix properly is set */
    void error(ParseErrorWithFileNLine e) {
        throw prefix(e)
    }

    /** Create generic ChangeLogParseException with the message msg prefixed with the changeId
        For specific cases specific Exception shall be used
     */
    ParseErrorWithFileNLine changeLogParseException(String msg, Throwable t=null) {
        new ParseErrorWithFileNLine(msg, changeId, t)
    }

    NullChecker<Tag> chkMap(Tag t, Map<String, Object> args = [:]) {
        new NullChecker<Tag>(t, args)
    }

    /** object to allow fluently call {@link #call} */
    class NullChecker<E extends Enum<E>>{
        final E elem
        public final Map<String, Object> asMap
        NullChecker(E e, Map<String, Object> args = [:]) {
            elem = e
            asMap = args
        }

        /** If `value` not null & `key` is not in the map yet put them in the map
            @throws ArgumentSetTwice if key is in the map already
         */
        NullChecker<E> call(String key, value) throws ArgumentSetTwice {
            ifNotNull(value){
                if(asMap.get(key)){
                    throw new ArgumentSetTwice(elem as String, key, changeId)
                }
                asMap.put(key, it)
            }
            this
        }
    }

    /** {@link #argsToMap} */
    Map<String, Object> argsAsMap(Tag tag, Object... args ) {
        String name = tag as String
        MethodDef method = methodDef(name)
        argsAsMap name, method, args
    }

    /** {@link #argsToMap}
    Map<String, Object> argsAsMap(String methodName, Object... args ) {
        Method method = methodDefs[methodName]
        argsAsMap method, requiresClosure(method), args
    }
    */
    /** {@link #argsToMap} */
    Map<String, Object> argsAsMap(String name, MethodDef method, Object... args ) {
        argsToMap changeId, name, method.needsClosure, method.toString(),
                method.args*.name, args
    }

    Map<String, Object> argsToMap( String methodName, boolean needsClosure,
                                         String fnDef, List<String> argNames, Object... args ) {
        argsToMap(changeId, methodName, needsClosure, fnDef, argNames, args )
    }

    /**
     * Generates a map: argNames[i] -> args[i] skips null values
     * Skips last args if {needsClosure} true
     * @param args expected to get all arguments including the starting Map and closing Closure
     * @param argNames expected to contain all parameter names excluding the first Map parameter
     * @throws InvalidArguments if mandatory parameter(s) are missing or there are more args than expected
     * @throws ArgumentSetTwice
     * @throws MissingClosure if {needsClosure} true and the last args not Closure
     *
     */
    static Map<String, Object> argsToMap(String prefix, String methodName, boolean needsClosure,
                                         String fnDef, List<String> argNames, Object... args )
        throws ArgumentSetTwice, InvalidArguments, MissingClosure {
        assert argNames.size() > 0
        // Make sure there are at least 1 args if needed
        if( !args || !args.length ) {
            throw needsClosure ? new MissingClosure(methodName, prefix)
                   : new InvalidArguments(methodName, fnDef, prefix, args)
        }

        int cl = args.last() instanceof Closure ? 1 : 0
        if(needsClosure && !cl) {
            throw new MissingClosure(methodName, prefix)
        }
        int i = args.first() instanceof Map ? 1 : 0
        Map<String, Object> map = i ? (Map)args.first(): new LinkedHashMap<>()
        // Make sure there are no more args, than expected
        if(args.length - i > argNames.size()) {
            throw new InvalidArguments(methodName, fnDef, prefix, args)
        } // TODO check if map contains only known args
        for(int n = 0; i < args.length-cl; i++) {
            String name = argNames[n++]
            def val = args[i]
//            if(val instanceof Closure) { // Any argument can be a closure
//                val = (val as Closure)()
//            }
            if(val != null) {
                if(map[name] != null){
                    throw new ArgumentSetTwice(methodName, name, prefix)
                }
                map[name] = val
            }
        }
        map
    }

    protected void logTagPropertyNotYetSupportedWarning(Tag tag, String attrib, String minVersion, String actLbVersion) {
        logWarning(prefix "Property '$attrib' of '$tag' ignored beacuse Liquibase v'$minVersion' supported first. v'$actLbVersion' not")
    }

    /** Check if all arguments are known for the {@code tag} in {@code args} using the method
      definition.
      Using the {code @Since} annotation on the argument definition recognises if the old name of
      the property is set only in {@code args}. <i>Sets value with the actual name and removes the
      entry with the old name</i><br>
      throws {@link InvalidArguments} with the list of unknown argument names
      throws {@link ArgumentSetTwice} if both the an argument's actual and old name are defined in {@code args}
     */
    protected void validateArgs(Tag tag, Map<String, Object> args)
       throws InvalidArguments, ArgumentSetTwice {
        MethodDef method = methodDef(tag.name())
        if(method) {
            Set<String> unsupportedKeys = args.keySet() - method.args*.name
            if(unsupportedKeys) { // Check alternative name
                method.args.each { String altName = it.getDeclaredAnnotation(Since)?.oldName()
                    if(altName) {
                        if(args[it.name]) {
                            error new ArgumentSetTwice(tag.name(), it.name, null, altName)
                        } else { // Silently set
                            unsupportedKeys -= altName
                            args[it.name] = args[altName]
                            args.remove(altName)
                        }
                    }
                }
                if(unsupportedKeys) {
                    error new InvalidArguments(tag.name(), method.toString(), unsupportedKeys)
                }
            }
        }
    }

    /** Log a warning prefixed with the change id using Liquibase's logger*/
    void logWarning(String msg) {
        getCurrentScope().getLog(getClass()).warning(prefix(msg))
    }

    static String fullChangeSetId(ChangeSet changeSet){"changeSet $changeSet"}

    /** Wrapper for PatchedObjectUtil.setProperty adds detailed error message */
    protected <T extends LiquibaseSerializable> T setProp(T entity, String name, Object value) {
        try {
            if(value != null) {
                PatchedObjectUtil.setProperty(entity, name, expandExpressions(value))
            }
            entity
        } catch (RuntimeException e) {
            throw new InvalidAttribute(entity.serializedObjectName, name, changeId, entity.serializableFields.toListString(), parent, e)
        }
    }

    @PackageScope <T extends LiquibaseSerializable> T setProps(T entity, Map<String, Object> props) {
        props.each {key, value ->
            setProp(entity, key, value)
        }
        entity
    }

    <V> V expandExpressions(V value) {
        value instanceof String ?
        DelegateUtil.expandExpressions(value, databaseChangeLog) as V : value
    }

    //@TypeChecked(TypeCheckingMode.SKIP)
    protected def propertyMissing(String name) {
        methodMissing name, null // Simply forward to methodMissing
    }

    UnrecognizedElement unrecognizedElement(String name){
        new UnrecognizedElement(name, knownElements())
    }

    @PackageScope InvalidAttribute InvalidAttribute(Tag tag, String name) {
        MethodDef m = methodDefs[tag.name()]
        new InvalidAttribute(tag.name(), name, changeId, m.toString(), parent)
    }

    /**
     * Groovy calls methodMissing when it can't find a matching method to call.
     * We use it to tell the user which changeSet had the invalid element.
     * @param name the name of the method Groovy wanted to call.
     * @param params the original arguments to that method.
     */
    protected def methodMissing(String name, params) {
        MethodDef method = methodDef(name)
        if(!method) {
            error unrecognizedElement(name)
        }
        callSingleMapArgVersion(name, method, params as Object[])
    }

    protected def callSingleMapArgVersion(String name, MethodDef method, Object[] args) {
        Map map = argsAsMap(name, method, args)

        // Make sure it exists to avoid infinite loop
        def m = metaClass.pickMethod(name, (method.lastArgClosure ? [Map, Closure] : [Map]) as Class[])
        if(!m) {
            throw new ParseErrorWithFileNLine("Unable to find method: '$name' for object $this with args: Map, Closure", changeId)
        }

        method.lastArgClosure ? m.invoke (this, objArr(map, args.last())) : m.invoke (this, map)
    }

    Map<String, Object> mergeNotNulls(Tag elem, Map<String, Object> m, Map<String, Object> args) {
        args.each { key, val ->
            if(val != null) {
                if(null != m.putIfAbsent(key, val)){
                    throw new ArgumentSetTwice(elem.name(), key, changeId)
                }
            }
        }
        m
    }

    static Map<String, Object> soMap(Map m) { m as Map<String, Object>}
}

@CompileStatic
@TupleConstructor(useSetters = true, includes = ['args']) //'name',
class MethodDef {
    //final String name
    Parameter[] args
    boolean needsClosure = true
    boolean lastArgClosure // Cache
    int argCount() { args ? args.size() : 0}

    void setArgs(Parameter[] params) {
        this.lastArgClosure = isClosure(params.last())
        needsClosure &= this.lastArgClosure
        args = params
    }

    static boolean isClosure(Parameter p) { p.type == Closure }

    /** Create human readable list of parameter names + types
     * Expects Closure to be the last parameter */
    @Override
    String toString() {
        args.inject(new CollectionStringBuilder()) { r, p ->
            switch ( p.type.simpleName ) {
                case 'Closure': return r << "{ $p.name }"
                    break
                case 'Map': break
                default :
                    r << "$p.type.simpleName $p.name"
            }
            r
        }
    }
}
