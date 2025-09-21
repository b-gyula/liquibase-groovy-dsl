package liquibase

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FirstParam
import liquibase.changelog.ChangeLogParameters
import liquibase.parser.ChangeLogParser
import liquibase.parser.ChangeLogParserFactory
import liquibase.parser.core.ParsedNode
import liquibase.parser.core.xml.AbstractChangeLogParser
import liquibase.parser.groovy.exception.UnrecognizedElement
import liquibase.resource.FileSystemResourceAccessor
import liquibase.resource.ResourceAccessor
import org.liquibase.groovy.delegate.*
import org.liquibase.groovy.delegate.DelegateUtil.CollectionStringBuilder

import java.lang.reflect.Method
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

import static liquibase.parser.ext.GroovyLiquibaseChangeLogParser.getMethods
import static liquibase.util.PatchedObjectUtil.convert
import static org.liquibase.groovy.delegate.Delegatee.methodDefs
import static org.liquibase.groovy.delegate.Delegatee.methodDefs4Tag

@CompileStatic
@groovy.util.logging.Log
@TupleConstructor(defaults = false)
class Convert2Groovy {
	static void main(String[] args) {
		if (args.size() > 0 && !args[0].isBlank()) {
			String fileName = args[0].trim()
			File outFile = null
			if (args.size() > 1) {
				if (args[1] != "-") {
					outFile = outputFile(args[1])
				}
			} else { // Same as input file
				outFile = outputFile(changeExtension(fileName))
			}

			def output = outFile ? new PrintStream(outFile) : System.out

			if(run(fileName, output)) {
				exit(0, (outFile ? "${outFile.getAbsoluteFile()} created successfully" : "") as String)
			}
			else {
				exit(2, '$ext format not supported')
			}
		} else {
			exit(1,"Usage: Convert2Groovy <input file> [<out file>] | -")
		}
	}

	static String changeExtension(String fileName) {
		int dot = fileName.lastIndexOf('.')
		fileName[0..dot] + 'groovy'
	}

	static ParsedNode run(String fileName, PrintStream output, ResourceAccessor ra = new FileSystemResourceAccessor('.')) {
		ParsedNode node = parse(fileName, ra)
		if (node) {
			IndentPrinter out = new IndentPrinter(new PrintWriter(output, true, StandardCharsets.UTF_8), '   ', true, true)
			out.println(headdr)
			try {
				serialize(node, new TagInfo(getMethods(GroovyScript), ['schemaLocation']), out)
				//test(o)
			} finally {
				out.flush()
			}
		}
		node
	}

	static ParsedNode parse(String fileName, ResourceAccessor ra) {
		ChangeLogParser parser = ChangeLogParserFactory.getInstance().getParser(fileName, ra)
		AbstractChangeLogParser p = DelegateUtil.cast( parser, AbstractChangeLogParser)
		if(!p) {
			return null
		}

		Method parseToNode = p.class.declaredMethods.find({it.name == 'parseToNode'})
		def b = parseToNode.trySetAccessible() // parseToNode protected
		parseToNode.invoke( p, fileName, new ChangeLogParameters(), ra) as ParsedNode
	}

	static exit(int exitCode, String msg) {
		println(msg)
		System.exit(exitCode)
	}

	static String headdr = """@groovy.transform.BaseScript(GroovyScript)
import liquibase.GroovyScript
"""

	static File outputFile(String fName){
		Path f = Path.of(fName)
		Files.createDirectories(f.toAbsolutePath().parent)
		Files.deleteIfExists(f)
		f.toFile()
	}

	@TupleConstructor
	static class TagInfo{
		final Map<String, MethodDef> methods
		final List<String> skip
		TagInfo(Map<String, MethodDef> m, List<String> skip = null){
			methods = m
			this.skip = skip
		}
	}

	static TagInfo tagInfo(String tagName) {
		_tagInfoMap.computeIfAbsent(tagName, {
			try {
				return new TagInfo(methodDefs4Tag(tagName))
			} catch (Exception e) {
				log.fine("Unable to get Delegate class for $tagName")
			}
			null
		})
	}

	protected static Map<String, TagInfo> _tagInfoMap = [
//		databaseChangeLog: new TagInfo(getMethodDefs(DatabaseChangeLogDelegate))
//		,changeSet: new TagInfo(getMethodDefs(ChangeSetDelegate))
		column: new TagInfo(methodDefs(ConstraintDelegate))
		,preConditions: new TagInfo(methodDefs(PreconditionDelegate))
		,not: new TagInfo(methodDefs(PreconditionDelegate))
		,and: new TagInfo(methodDefs(PreconditionDelegate))
		,or: new TagInfo(methodDefs(PreconditionDelegate))
	]

	static Map<String, Map<String, String>> replaced = [
		changeSet: [contextFilter: 'context'] as Map<String, String>
	]

	static String altAttrib(List<ParsedNode> children, String nodeName, String arg, Class cls) {
		def reps = replaced[nodeName]
		if(reps) {
			String altName = reps[arg]
			return altName ? getNRemove(children, {altName.equals(it.name)}, cls) : null
		}
	}

	static def quot = ~/'|\n/

	static String asString(Object value) {
		if(value == null) return null
		String s = value.toString()
		switch (value) { // TODO Enum
			case Boolean:
			case Number:
			//case Enum:
				return s
			default:
				if (s.contains('\n')) return "'''${value}'''"
				s= s.replace('\\', '\\\\')
				if (s.contains("'")) return '"' + s.replace('"', '\\"') + '"'
		}
		return "'${s.replace('"', '\\"')}'"
	}

	static def unrecognizedElement(String name, Collection<String> knownElements) {
		log.warning(new UnrecognizedElement(name, knownElements).message)
	}

	static <T> String getNRemove(List<ParsedNode> nodes, @ClosureParams(FirstParam.FirstGenericType) Closure <Boolean> chk,
										  Class<T> cls, @ClosureParams(FirstParam.FirstGenericType) Closure process = null) {
		def it = nodes.iterator()
		while (it.hasNext()) {
			def n = it.next()
			if(chk(n)) {
				it.remove()
				String r = asString(convert(n.value,cls))
				if(null == process) {
					return r
				} else {
					process(n)
				}
			}
		}
		null
	}

	/**
	 *
	 * @param node
	 * @param info
	 * @param o
	 * @param posArgs the max positional parameters
	 */
	static List<String> serialize(ParsedNode node, TagInfo info, IndentPrinter o, int posArgs = 3, List<String> includes = []) {
		List<ParsedNode> children = node.children.inject(new LinkedList<>()) { l, n ->
			if(info?.skip?.contains(n.name)) {
				log.info("Skipped '$n' as per config")
			} else {
				l.add (n)
			}
			l
		}
		if(!children){ // Just use value as single parameter
			println o, "$node.name " + asString(node.value)
		} else {
			CollectionStringBuilder params = new CollectionStringBuilder()
			MethodDef m = info ? info.methods[node.name] : null
			if(m) {
				m.args.each { // known positional value parameter -> write out in expected order
					if (!MethodDef.isClosure(it)) {
						String val = altAttrib(children, node.name, it.name, it.type) ?:
							getNRemove(children, { n -> it.name.equals(n.name) }, it.type)
						if (null != val) { // Until the first not found
							params << (!posArgs ? "$it.name:" : '') + val
							if(posArgs > 0) posArgs--
						} else {
							posArgs = 0 // No more positional params
						}
					}
				}
			} else {
				unrecognizedElement(node.name, info?.methods?.keySet() )
			}
			TagInfo childTags
			if(children) { // Add remaining not known values as params
				childTags = tagInfo(node.name) // Unknown or known child
				getNRemove(children, { (!childTags ||!childTags.methods.containsKey(it.name)) && it.value }, String,
					{ params << "$it.name:" + asString(it.value) }
				)
			}
			print(o,"$node.name $params") // Done with attribs
			if(children || node.value) { // Real children
				if (!m?.lastArgClosure) {
					log.severe("No child expected for `$node.name`: ${children.join(",")} !!")
				} //else {
					if (params.size() > 0) o.print ', '
					o.print "{\n"
					o.incrementIndent()
					children.each {
						serialize(it, childTags, o)
					}
					if(node.value) {
						println(o, asString(node.value))
					}
					o.decrementIndent()
					o.println "}"
				//}
			}
			else {
				if (m?.needsClosure) {
					log.severe("Missing expected children for `$node.name`: ${m} !!")
				}
				o.println()
			}
		}
	}

	// IndentPrinter error: calling print with interpolated string like "$v"
	// prints to `Sytem.out` insted in the contained OutputStream""
	static void print(IndentPrinter o, String s) {
		o.printIndent()
		o.print s
	}

	static void println(IndentPrinter o, String s) {
		o.println(s)
	}

}
