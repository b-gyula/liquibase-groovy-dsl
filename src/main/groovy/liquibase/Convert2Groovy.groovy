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

	/** Convert the changelog XML given as the first parameter to groovy format
	 into the file given as the optional 2nd parameter

	 All included XMLs are also converted generating the same folder structure

	 */
	static void main(String[] args) {
		if (args.size() > 0 && !args[0].isBlank()) {
			String fileName = args[0].trim()
			File outFile = null
			if (args.size() > 1) {
				outFile = outputFile(args[1])
			}

			if(run(fileName, new FileSystemResourceAccessor('.'), outFile)) {
				exit(0,)
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

	static File run(String fileName, ResourceAccessor ra, File outFile = null) {
		ParsedNode node = parse(fileName, ra)
		if (node) {
			processIncludes node, ra, Path.of(fileName)
			if(!outFile) outFile = outputFile(changeExtension(fileName), ra)
			IndentPrinter out = new IndentPrinter(new PrintWriter(new PrintStream(outFile)
				, true, StandardCharsets.UTF_8), '   ', true, true)
			out.println(headdr)
			try {
				serialize(node, out)
				System.out.println("${outFile.getAbsoluteFile()} created successfully")
			} finally {
				out.flush()
			}
			return outFile
		}
		null
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

	static exit(int exitCode, String msg = null) {
		if(msg) println(msg)
		System.exit(exitCode)
	}

	static String headdr = """@groovy.transform.BaseScript(GroovyScript)
import liquibase.GroovyScript
"""

	// TODO ra relative output does not work
	static File outputFile(String fName, ResourceAccessor ra = null){
		//Path f = Path.of(ra ? ra.get(fName).uri.toString().replaceFirst("^\\w+:/","") :fName)
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
		 script: new TagInfo(getMethods(GroovyScript), ['schemaLocation'])
		,databaseChangeLog: new TagInfo(getMethodDefs(DatabaseChangeLogDelegate))
		,changeSet: new TagInfo(getMethodDefs(ChangeSetDelegate))
		,column: new TagInfo(methodDefs(ConstraintDelegate))
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
		return "'${s.replace("'", "\\'")}'"
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
	static void serialize(ParsedNode node, IndentPrinter o, TagInfo info = tagInfo('script'), int posArgs = 3) {
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
						serialize(it, o, childTags)
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

	static retaltiveToLogFile(ParsedNode n) {
		n.getChildValue(null,'relativeToChangelogFile', Boolean)
	}

	static String mkRelative(Path logFile, String fileName, ParsedNode parentNode) {
		if(retaltiveToLogFile(parentNode)) {
			Path parent = logFile.parent
			if(parent) {
				fileName = parent.resolve(fileName)
			}
		}
		fileName
	}

	static void processIncludes(ParsedNode node, ResourceAccessor ra, Path thisFile) {
		node.children.each {
			if('include' == it.name) {
				ParsedNode fileNode = it.getChild(null, 'file')
				String oFileName = fileNode.getValue(String)
				String fileName = mkRelative thisFile, oFileName, it
				if(fileName.endsWith('.xml')) {
					File outFile = run( fileName, ra)
					fileNode.setValue(changeExtension(oFileName))
				}
			} else if ('includeAll' == it.name) {
				String path = it.getChildValue(null, 'path', String)
				String pathName = mkRelative thisFile, path, it
				def opts = new ResourceAccessor.SearchOptions()
				opts.minDepth = it.getChildValue(null, 'minDepth', 0)
				opts.maxDepth = it.getChildValue(null, 'maxDepth', Integer.MAX_VALUE)
				opts.setTrimmedEndsWithFilter('.xml')
				ra.search(pathName,opts).each {
					run( it.path, ra)
				}
			}
		}
	}

}
