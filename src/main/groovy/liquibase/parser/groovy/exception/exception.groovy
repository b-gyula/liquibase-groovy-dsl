package liquibase.parser.groovy.exception

import groovy.transform.CompileStatic
import liquibase.exception.ChangeLogParseException;

@CompileStatic
/** Helper class to add prefix, file name and line number to the exception after created (not to deepen the stacktrace ) */
class ParseErrorWithFileNLine extends ChangeLogParseException {
    String fileNameAndLine = ""
    String prefix

    ParseErrorWithFileNLine(String msg, String prefix){
        super(msg)
        this.prefix = prefix
    }

    @Override
    String getMessage() { (prefix ? prefix+': ':'') + super.message + fileNameAndLine }
}

@CompileStatic
class MissingClosure extends ParseErrorWithFileNLine {
    MissingClosure(String tagName, String prefix = null) {
        super("'$tagName' missing required closure", prefix)
    }
}

@CompileStatic
class ArgumentSetTwice extends ParseErrorWithFileNLine {
    ArgumentSetTwice(String tagName, String argName, String prefix = null) {
        super("Cannot set argument '$argName' twice for '$tagName'! Set either positional or named argument only!", prefix)
    }
}

@CompileStatic
class UnrecognizedElement extends ParseErrorWithFileNLine {
    UnrecognizedElement(String tagName, Collection<String> knownElements, String prefix = null,
            String msg = "Unrecognized element: '$tagName'! Valid elements are ${knownElements.toListString()}"
    ) {
        super(msg, prefix)
    }
}

@CompileStatic
class InvalidArgument extends ParseErrorWithFileNLine {
    InvalidArgument(String tagName, String fnDef, String prefix = null, Object[] args) {
        super("'$tagName' element got invalid arguments ${argsToString(args)}. Valid arguments are: $fnDef", prefix )
    }

    static String argsToString(Object[] args){
        if(null == args ) {
            return '[]'
        }
        args.inject("["){ String acc, val ->
            if(acc.length() > 1){
                acc += ','
            }
            String v
            if(val instanceof Closure) v = '{}'
            else if(val instanceof String) v = "'$val'"
            else v = val.toString()
            acc + v
        } + ']'
    }
}
