import com.sun.org.apache.xerces.internal.impl.xs.XMLSchemaLoader
import com.sun.org.apache.xerces.internal.impl.xs.XSElementDecl
import com.sun.org.apache.xerces.internal.xni.grammars.XMLGrammarDescription
import com.sun.org.apache.xerces.internal.xs.*
import groovy.namespace.QName
import groovy.text.GStringTemplateEngine
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.xml.XmlParser
import jdk.jshell.spi.ExecutionControl
import liquibase.database.ObjectQuotingStrategy

import static com.sun.org.apache.xerces.internal.xs.XSConstants.*
import static ChangeGenerator.Name.*

@CompileStatic
@groovy.util.logging.Log
class ChangeGenerator {
//    static final Map<String, Class> enumType = [
//            ObjectQuotingStrategy: ObjectQuotingStrategy] as Map<String, Class>


    static interface Name {
        String PreConditionChildren = 'PreConditionChildren'
        String liquibaseNS = 'http://www.liquibase.org/xml/ns/dbchangelog'
    }

    static XmlParser xmlParser = new XmlParser()

    static void main(String[] args) {
        File xsd = new File(args.length > 0 ? args[0] : 'D:\\dev\\liquibase\\XSD.remove.invalid.root.elements\\liquibase-standard\\src\\main\\resources\\www.liquibase.org\\xml\\ns\\dbchangelog\\dbchangelog-latest.xsd')
/*SAXParserFactory saxParserFactory = SAXParserFactory.newInstance()
        Schema schema =
                SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
                .newSchema(xsd)
        factory.schema = schema*/
        XSModel model = new XMLSchemaLoader().loadURI(xsd.toURI().toString())
//        XSGrammar gr = model.namespaceItems.find {
//            XSGrammar gr = it as XSGrammar
//            XMLGrammarDescription s = gr.grammarDescription
//            s.namespace == liquibaseNS
//        }
        //namespaces.get(0)def s = ns.documentLocations
        String path = '.'

        /** List of global defined model group names and the elements to skip */
        [PreConditionChildren:[]].each { modelGroup, skipList ->
            List<Method> methods = []
            model.getModelGroupDefinition(modelGroup, liquibaseNS)
                    .modelGroup
                    .particles.each {
                XSParticle e = it as XSParticle
                def t = e.term
                if ( !skipList.contains(t.name) )
                    switch ( t ) {
                        case XSElementDecl: methods += process(t as XSElementDecl, new Method(t.name))
                            break
                    }
                }


            File out = new File(path, "${modelGroup}.groovy")
            if ( out.exists() ) {
                out.delete()
            }
            out << new GStringTemplateEngine()
                    .createTemplate(new File("${modelGroup}.templ"))
                    .make([methods: methods, timestamp: timestamp()])
        }
    }

    static String timestamp() {
        "/* Generated @ ${new Date()} */"
    }

    static String annotations2String(XSObjectList objs) {
        objs ? objs.sum {
            if(it instanceof XSAnnotation) {
               return annotations2String(it as XSAnnotation)
            }
            ''
        }
        :''
    }

    static String annotations2String(XSAnnotation ann) {
        ann ? getDoc(ann.annotationString): ''
    }

    static String getDoc(String annText) {
        def annotation = xmlParser.parseText(annText)
                .getAt(new QName(XMLGrammarDescription.XML_SCHEMA,'documentation'))
                .text()
        annotation
    }

    @TupleConstructor(useSetters = true)
    static abstract class NameAndDesc<This>{
        final String name
        String desc = ''

        This setDesc(String s) {
            if(desc) {
                log.warning "desc is already set for $name: $desc got: $s"
            }
            desc += s.strip().replaceAll('\n[ ]+','\n\t\t')
            this as This
        }
    }

    @TupleConstructor(includeSuperProperties=true,callSuper=true)
    static class ArgDef extends NameAndDesc<ArgDef>{
        boolean required
        String type
        String alias
        //String since

        /**  See {@link com.sun.org.apache.xerces.internal.xs.XSConstants} */
        ArgDef setType(XSSimpleTypeDefinition td) {
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
                    throw new ExecutionControl.NotImplementedException("builtInKind: $td.builtInKind")
            }
            //}
            this
        }
    }

    @TupleConstructor(includeSuperProperties=true,callSuper=true)
    static class Method extends NameAndDesc<Method> {
        boolean hasChild
        final List<ArgDef> args = []
        boolean filterParams(ArgDef arg, boolean skipArgsWithNoDesc = true) {
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
               """\n|\t <br>Params:<dl>
                ${filtered.sum{
            """|\t <dt>${it.name}</dt>
               |\t\t<dd>${it.desc}</dd>""" }}
               |\t</dl>"""
              :filtered.sum{"\n|\t  @param $it.name $it.desc\n\t"} )
        }

        String argList(boolean withTypeAndDefaults = false, Closure<String> closureType = {''}) {
            String r = args.sum{
                "${withTypeAndDefaults ? it.type: ''} $it.name${withTypeAndDefaults && !it.required ? '=null' : ''}, "}
            hasChild ? r + (withTypeAndDefaults ? closureType() + "Closure " :'') + "closure"
                    : r.dropRight(2) // Cut last ,
        }


        String javadoc(boolean argsAsHtml = false, boolean skipArgsWithNoDesc = true) { """
            |\t/** ${desc} ${argDescs(argsAsHtml,skipArgsWithNoDesc)} */""".stripMargin()
        }

        String toString() {name}
    }

    /**
     *
     * @param decl
     * @param m
     * @return
     */
    static Method process (XSElementDecl decl, Method m) {
        m.setDesc annotations2String(decl.annotation)
        switch ( decl.typeDefinition ) {
            case XSComplexTypeDefinition:
                XSComplexTypeDefinition typeDef = (XSComplexTypeDefinition)decl.typeDefinition
                m.desc += annotations2String(typeDef.annotations)
                m.hasChild = typeDef.contentType & XSComplexTypeDefinition.CONTENTTYPE_ELEMENT
                typeDef.attributeUses.each {
                    XSAttributeUse attr = it as XSAttributeUse
                    ArgDef arg = new ArgDef( attr.attrDeclaration.name
                            , annotations2String(attr.annotations)
                            , attr.required)
                            .setType(attr.attrDeclaration.typeDefinition as XSSimpleTypeDefinition )
                    m.args << arg
                }
        }
        m
    }
}
