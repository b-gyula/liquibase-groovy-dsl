import com.sun.org.apache.xerces.internal.xs.XSSimpleTypeDefinition
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor

import java.util.regex.Pattern

import static com.sun.org.apache.xerces.internal.xs.XSConstants.BOOLEAN_DT
import static com.sun.org.apache.xerces.internal.xs.XSConstants.ENTITY_DT
import static com.sun.org.apache.xerces.internal.xs.XSConstants.INTEGER_DT
import static com.sun.org.apache.xerces.internal.xs.XSConstants.LONG_DT
import static com.sun.org.apache.xerces.internal.xs.XSConstants.NEGATIVEINTEGER_DT
import static com.sun.org.apache.xerces.internal.xs.XSConstants.NOTATION_DT
import static com.sun.org.apache.xerces.internal.xs.XSConstants.STRING_DT
import static ChangeGenerator.*

@CompileStatic
@groovy.util.logging.Log
@TupleConstructor(useSetters = true)
abstract class NameAndDesc<This>{
    final String name
    String desc = ''

    This setDesc(String s) {
        if(s) {
            if ( desc ) {
                log.warning "desc is already set for $name: $desc got: $s"
            }
            desc += corrections.inject(s.strip()) { String str, Tuple2<Pattern, String> c ->
                //str.replaceAll(c.v1, c.v2)
                c.v1.matcher(str).replaceAll(c.v2)
            }
        }
        this as This
    }
}

@CompileStatic
@groovy.util.logging.Log
@TupleConstructor(includeSuperProperties=true, callSuper=true, useSetters = true)
class Method extends NameAndDesc<Method> {
    final List<Arg> args = []
    boolean hasChild
    boolean hasRequired() {args.any { it.required}}

    boolean childOptional() { hasChild && !args.last().required }

    boolean filterParams(Arg arg, boolean skipArgsWithNoDesc = true) {
        boolean hasDesc = arg.desc
        if(!hasDesc) {
            log.info "$name / $arg.name has no description"
        }
        hasDesc || !skipArgsWithNoDesc
    }

    String argDescs(boolean asHtml, boolean skipArgsWithNoDesc = true) {
        def filtered = args.findAll{filterParams(it, skipArgsWithNoDesc)}
        !filtered ? '' :
                (asHtml ?
                        """\n|\t <br>Params:<dl>${filtered.sum{
                            """\n|\t <dt>${it.nameAsHtml()}</dt>
               |\t\t<dd>${it.desc}</dd>""" }}
               |\t</dl>"""
                        :filtered.sum{"\n|\t  @param $it.name $it.desc\n\t"} )
    }

    /** Generate the argument list
     * @param addType add type
     */
    String argList( boolean addTypeAndDefault = false, boolean skipOptionalChild = false ) {
        String r = args.dropRight( childOptional() ? 1 : 0 ).sum{
            it.toString(addTypeAndDefault, addTypeAndDefault) + ','
        }
        if(childOptional() && !skipOptionalChild) {
            r += args.last().toString(addTypeAndDefault, false)
        } else {
            r = r.dropRight(1) // Cut last ,
        }
        r
    }

    String javadoc(boolean argsAsHtml = false, boolean skipArgsWithNoDesc = true) { """
            |\t/** ${desc} ${argDescs(argsAsHtml, skipArgsWithNoDesc)} */""".stripMargin()
    }

    String toString() {name}

    @TupleConstructor(includeSuperProperties=true, callSuper=true)
    static class Arg extends NameAndDesc<Arg>{
        boolean required = false
        String type = 'String'
        //String alias
        //String since

        /**  See {@link com.sun.org.apache.xerces.internal.xs.XSConstants} */
        boolean setType(XSSimpleTypeDefinition td) {
            if(td.isDefinedFacet(XSSimpleTypeDefinition.FACET_ENUMERATION)) {
                type = td.name
            } else
                switch ( td.builtInKind ) { //
                    case STRING_DT:
                    case NOTATION_DT..ENTITY_DT: type = 'String'
                        break
                    case BOOLEAN_DT: type = 'Boolean'
                        break
                    case INTEGER_DT..NEGATIVEINTEGER_DT: type = 'Integer'
                        break
                    case LONG_DT: type = 'Long'
                        break
                    default:
                        switch( td.name) {
                            case 'booleanExp': type = 'Boolean'
                                break
                            case 'integerExp': type = 'Integer'
                                break
                            case 'propertyName': type = 'String'
                                break
                            default:
                                return false
                        }
                }
            true
        }

        String nameAsHtml() {
            required ? "<b>$name</b>" : name
        }

        String toString(boolean addType = false, boolean addDefault = false) {
            "${addType ? ' ' + type : ''} $name${addDefault && !required ? '=null' : ''}"
        }
    }
}
