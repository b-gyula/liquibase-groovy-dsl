import com.sun.org.apache.xerces.internal.impl.xs.XMLSchemaLoader
import com.sun.org.apache.xerces.internal.impl.xs.XSElementDecl
import com.sun.org.apache.xerces.internal.xni.grammars.XMLGrammarDescription
import com.sun.org.apache.xerces.internal.xs.*
import groovy.namespace.QName
import groovy.text.GStringTemplateEngine
import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.xml.XmlParser
import org.liquibase.groovy.delegate.ChangeSetDelegate

import static groovy.lang.Tuple.tuple as t
import static org.liquibase.groovy.delegate.DelegateUtil.cast
import static ChangeGenerator.Name.*
import static Method.Arg

@CompileStatic
@groovy.util.logging.Log
@Newify(File)
class ChangeGenerator {

	static interface Name {
		//String PreConditionChildren = 'PreConditionChildren'
		String liquibaseNS = 'http://www.liquibase.org/xml/ns/dbchangelog'
	}

	static XmlParser xmlParser = new XmlParser()

	static final Map<String, String> closureType = ChangeSetDelegate.closureDelegate.collectEntries {
		k, v -> [k, v.simpleName]
	}

	static void main(String[] args) {
		File xsd = File(args.length > 0 ? args[0] : 'dbchangelog-latest.xsd')
		File outPath = File(args.length > 1 ? args[1] : '.') // 'src/main/groovy/org/liquibase/groovy/delegate'
		if (!outPath.exists()) {
			log.severe("Output path $outPath.absolutePath does not exists!")
			System.exit(1)
		}

		File templPath = File('gen')

		XSModel model = new XMLSchemaLoader().loadURI(xsd.toURI().toString())
		List<Method> allMethods = []
		/** List of global defined model group names */
		['ChangeSetChildren'
		 ,'PreConditionChildren'
		].each { modelGroup ->
			List<Method> methods = getMethods([], model, modelGroup)
			generateFile(File(templPath, "${modelGroup}.templ")
							, File(outPath, "${modelGroup}.groovy")
							, methods)
			allMethods += methods
		}
/*		// Generate separate file about all params
		generateFile(File(templPath, "Params.templ")
			, File("Params.groovy")
			, allMethods)*/
	}

	static List<Method> getMethods(List<Method> methods, XSModel model, String modelGroup) {
		model.getModelGroupDefinition(modelGroup, liquibaseNS)
			.modelGroup
			.particles.each {
			XSTerm t = (it as XSParticle).term
			//if ( !skipList.contains(t.name)) {
			switch (t) {
				case XSElementDecl: methods += process(t as XSElementDecl, new Method(t.name))
					break
			}
//                } else {
//                    log.info("$t.name filtered out by the skipList")
//                }
		}
		methods
	}

	static void generateFile(File templ, File out, List<Method> methods) {

		if (!templ.exists()) {
			log.severe("Template $templ.absolutePath does not exists!")
			System.exit(2)
		}
		if (out.exists()) {
			out.delete()
		}
		out << new GStringTemplateEngine()
			.createTemplate(templ)
			.make([methods      : methods
					 , timestamp  : timestamp()
					 , closureType: closureType
			])
	}

	static String closureType(String tag) {
		String clType = closureType[tag]
		"\n\t\t\t\t@DelegatesTo(${clType ? "value=$clType," : ''} strategy=DELEGATE_ONLY) Closure"
	}

	static String timestamp() {
		"/* Generated @ ${new Date()} on ${InetAddress.localHost.hostName} */"
	}


	static String annotations2String(XSObjectList objs) {
		objs ? objs.sum {
			if (it instanceof XSAnnotation) {
				return annotations2String(it as XSAnnotation)
			}
			''
		}
			: ''
	}

	static String annotations2String(XSAnnotation ann) {
		ann ? getDoc(ann.annotationString) : ''
	}

	/** Parse documentation from annotation body because xerces does not parse it further */
	static String getDoc(String annText) {
		def annotation = xmlParser.parseText(annText)
			.getAt(new QName(XMLGrammarDescription.XML_SCHEMA, 'documentation'))
			.text()
		annotation
	}

	/** String corrections to perform on descriptions */
	static def corrections = [
		t(~'\n +', '\n\t\t'),
		t(~'@(\\S+)', '{@code \$1}'),
		t(~"\\*/", '*\\\\/')
	]

	/** Collect method definition from {@code elemDecl} in {@code m}
	 *
	 * @param elemDecl
	 * @param m
	 */
	static Method process(XSElementDecl elemDecl, Method m) {
		m.setDesc annotations2String(elemDecl.annotation)
		switch (elemDecl.typeDefinition) {
			case XSComplexTypeDefinition:
				XSComplexTypeDefinition typeDef = (XSComplexTypeDefinition) elemDecl.typeDefinition
				m.desc += annotations2String(typeDef.annotations)
				typeDef.attributeUses.each {
					XSAttributeUse attr = it as XSAttributeUse
					Arg arg = new Arg(attr.attrDeclaration.name
						, annotations2String(attr.annotations)
						, attr.required)
					XSSimpleTypeDefinition t = attr.attrDeclaration.typeDefinition as XSSimpleTypeDefinition
					if (!arg.setType(t)) {
						log.severe("Unable to set type from $t for $m.name / $arg.name")
						throw new Exception("Unable to set type from $t for $m.name / $arg.name")
					}
					m.args << arg
				}
				// Get the content element name as the last argName
				if (typeDef.contentType & XSComplexTypeDefinition.CONTENTTYPE_ELEMENT)
					switch (typeDef.particle?.term) { // Find the first element declaration that makes us treat as closure
						case XSModelGroup:
							XSModelGroup g = typeDef.particle?.term as XSModelGroup
							def elems = g.particles.findResults {
								def p = (XSParticle) it
								def e = cast(p.term, XSElementDecl)
								e ? new Child(p, e) : null
							}

							if (elems) {
								elems[0].with {
									m.args << new Arg(
										elems.size() > 1 ? 'closure' : elem.name + 's'// (particle.maxOccurs != 1 ? elem.name + 's' : elem.name)
										, annotations2String(particle.annotations)
										, particle.minOccurs > 0
										, elems.size() == 1 && particle.maxOccurs == 1 ? 'String' : // Use a simple attribute like sql comment -> Arg
										closureType(m.name))
									m.hasChild = elems.size() > 1 || particle.maxOccurs != 1
								}
							}
					}
		}
		m
	}
}

@TupleConstructor
class Child {
	XSParticle particle
	XSElementDecl elem
}

