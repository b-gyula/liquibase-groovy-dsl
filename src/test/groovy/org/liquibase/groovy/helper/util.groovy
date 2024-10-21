package org.liquibase.groovy.helper

import groovy.transform.TupleConstructor
import liquibase.changelog.ChangeLogParameters
import liquibase.changelog.DatabaseChangeLog
import liquibase.parser.ChangeLogParserFactory
import liquibase.parser.ext.GroovyLiquibaseChangeLogParser
import liquibase.parser.groovy.exception.*
import liquibase.precondition.Precondition
import liquibase.precondition.core.PreconditionContainer
import org.liquibase.groovy.delegate.Delegatee
import liquibase.resource.ResourceAccessor

import java.lang.reflect.Field

import static org.junit.Assert.assertEquals
import static liquibase.parser.ext.GroovyLiquibaseChangeLogParser.*


import java.nio.charset.StandardCharsets

@groovy.transform.CompileStatic
class util {

    /** parserFactory with GroovyLiquibaseChangeLogParser registered */
    static @Lazy ChangeLogParserFactory parserFactory = registerParser()
    static ChangeLogParserFactory registerParser() {
        ChangeLogParserFactory.instance.register(new GroovyLiquibaseChangeLogParser())
        ChangeLogParserFactory.instance
    }

    /** Get registered parser */
    static DatabaseChangeLog parseDatabaseChangeLog(File changeLogFile, ResourceAccessor resourceAccessor) {
        parserFactory.getParser(changeLogFile.path, resourceAccessor)
            .parse(changeLogFile.path, new ChangeLogParameters(), resourceAccessor)
    }

    /** Parse a changeLog from String {@code content} */
    static DatabaseChangeLog parseDatabaseChangeLog( ResourceAccessor resourceAccessor, String content) {
        parse(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)), resourceAccessor)
    }

    /** Parse a changeLog from String {@code content} */
    static DatabaseChangeLog parseDatabaseChangeLog(String content) {
        parseDatabaseChangeLog null, content
    }

    static File createFileFrom(File directory, String suffix, String text) {
        createFileFrom(directory, 'liquibase-', suffix, text)
    }

    /** Create a temporary file in the {@code directory} with the name {prefix}{suffix} containing the
     * {text}
     * @return the created file
     */
    static File createFileFrom(File directory, String prefix, String suffix, String text) {
        def file = File.createTempFile(prefix, suffix, directory)
        file << text
    }

    static IO io(String i, o) { new IO ( i, o)}
    @TupleConstructor()
    static class IO { String i; Object o }

    static failedCase(String c, Throwable t){
        throw new AssertionError("Case '$c' thrown: $t.class.simpleName: $t.message", t)
    }

    @Category(Delegatee)
    static class DelegateeCategory {

        String invalidElement(String name) {
            this.prefix( new UnrecognizedElement(name, this.knownElements())).message
        }

        /** Convert Tag to String + set prefix*/
        String missingClosure(Tag name) {
            this.prefix(new MissingClosure( name as String)).message
        }

        /** Convert Tag to String */
        String invalidArgs(Tag name, Object... args) {
            this.prefix(new InvalidArgument( name as String, '', args)).message
        }

        /** Convert Tag to String */
        String attributeSetTwice(Tag tag, String attribName ) {
            this.prefix(new ArgumentSetTwice( tag as String, attribName)).message
        }
    }

    /** Validate if all keys in {@link expected} are also in {@link actual} and the values are matching
        If the type do not match compare their String representation
      */
    static void assertMapEquals(Map<String, Object> expected, Map<String, Object> actual) {
        expected.each {
            def act = actual[it.key]
            def exp = it.value
            if(null != act && exp.class != act.class) {
                act = act as String // Convert actual
                if ( exp.class != String ) { // If we have only th string representation
                    exp = exp as String // Convert both to String
                }
            }
            assertEquals it.key, exp, act
        }
    }

    /** Get the last ChangeLogParameter using reflection
     * @param global Get the last global or local changeLogParameter
     * @return
     */
    static def lastParam(DatabaseChangeLog changeLog, boolean global = true) {
        // change log parameters are not exposed through the API, so get them using reflection.
        // Also, there are
        def changeLogParameters = changeLog.changeLogParameters
        Field f = changeLogParameters.class.getDeclaredField((global ? "global" : "local")+"Parameters")
        f.setAccessible(true)
        def properties = f.get(changeLogParameters)
        //def properties = m.invoke(changeLogParameters, key, null, null)//f.get(changeLogParameters)
        global ? (properties as List).last() :
                ((properties as Map).get(changeLog.physicalFilePath) as List).last() // The last one is ours.
    }

    static String makeRelativeTo(File f, String root) {
        // Now make it relative - add 1 to the index to eat the "/"
        f.path.replaceAll("\\\\", "/")
                .substring(root.length() +1)
    }

    /**
     * Helper method to extract the actual preconditions from a list of potential preconditions.
     * <p>
     * Liquibase often nests the actual preconditions in a precondition container.  This method
     * will walk through a collection of objects, extracting preconditions and recursively checking
     * nested items in a container to get just the preconditions themselves.
     * @param preconditions the collection of preconditions to search
     * @return a list of actual preconditions.
     */
    static List<Precondition> extractPreconditions(PreconditionContainer preconditions) {
        List<Precondition> actualPreconditions = []
        preconditions.nestedPreconditions.each { pc ->
            if ( pc instanceof PreconditionContainer ) {
                actualPreconditions.addAll extractPreconditions(pc)
            } else if ( pc instanceof Precondition) {
                actualPreconditions.add pc
            }
        }
        return actualPreconditions
    }
}
