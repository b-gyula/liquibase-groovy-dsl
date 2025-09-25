package liquibase

import groovy.transform.CompileStatic
import liquibase.changelog.DatabaseChangeLog
import liquibase.logging.Logger
import liquibase.parser.core.ParsedNode
import liquibase.resource.DirectoryResourceAccessor
import liquibase.resource.Resource
import liquibase.resource.ResourceAccessor

import java.nio.file.Files
import java.nio.file.Path

import static liquibase.parser.ext.GroovyLiquibaseChangeLogParser.dbChangeLogTagName
import spock.lang.Specification
import spock.lang.Unroll
import static Convert2Groovy.*
import static  BenchmarkTest.*

@Unroll
class Convert2GroovySpec extends Specification{
	def 'test asString'(){
		expect:
		asString( input ) == expected
		where:
		input	 | expected
		's"f'  | /'s"f'/
		"s/f"  | "'s/f'"
		"s'f"  | /"s'f"/
		//"s'/"	 | "/s'\\//"
		"s\nf" | /'''s
f'''/
//		/s\'\w/ /"s'\\\\w"/
		"s\\f" | "'s\\\\f'"
	}

	//@Unroll
	def sql () {
		String parent = 'changeSet'
		StringWriter out = new StringWriter()
		ParsedNode node = new ParsedNode(null, parent)
		serialize(node.setValue((Object)input), new IndentPrinter(out), tagInfo(dbChangeLogTagName))
		System.println(out)
		String expected = "changeSet {\n$exp\n}\n"
		expect:
		out.toString() == expected
		where:
		input	 			| exp
		[sql:'select'] | /sql 'select'/
	}

	/** Convert test xml (with its inludes) in new groovy format */
	void convert() {
		when:
		//File root =  new File('../build4/liquibase-integration-tests/src/test/resources')

		//File xml = new File (root, fileName)
		//String fileName = 'test.changelog.xml'
		String fileName = commonTestChangelogNoInc + '.xml'
		//File groovyFile = new File(changeExtension(fileName))
		//File groovyFile =  outputFile(changeExtension(fileName))

		ResourceAccessor ra = new FolderResourceAccessor(commonTestChangelogRoot)
		//ResourceAccessor ra = new DirectoryResourceAccessor(Path.of('src/test/resource'))

		run(fileName, ra)
		then:
		noExceptionThrown()
	}

	/** parse and load both the original test xmls and their converted version and compare the result */
	void validate() {
		String fileName = commonTestChangelogNoInc + '.xml'
		//String fileName = commonTestsChangelog + '.xml'
		ResourceAccessor ra = new FolderResourceAccessor(commonTestChangelogRoot)
		// Parse XML
		DatabaseChangeLog xmlLog = parseToChangeLog(fileName, ra)

		File groovyFile = new File(changeExtension(fileName))
		// Parse
		DatabaseChangeLog groovy = parseToChangeLog(groovyFile.path, new DirectoryResourceAccessor(Path.of('.')))

		expect:
		serialize(groovy) == serialize(xmlLog)
	}
}

@CompileStatic
class FolderResourceAccessor extends DirectoryResourceAccessor {

	FolderResourceAccessor(File directory) throws FileNotFoundException {
		super(directory)
	}

	@Override
	public List<Resource> getAll(String path) throws IOException {

		final List<Resource> returnList = new ArrayList<>();

		Logger log = Scope.getCurrentScope().getLog(getClass());
		path = new File(path).toPath().normalize().toString().replace("\\", "/")

		if (path == null) {
			return returnList;
		}
		Path finalPath = getRootPath().resolve(path);
		if (Files.exists(finalPath)) {
			returnList.add(createResource(finalPath, path));
		} else {
			log.fine("Path " + path + " in " + getRootPath() + " does not exist (" + this + ")");
		}

		return returnList;
	}
}