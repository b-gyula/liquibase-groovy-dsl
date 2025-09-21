package liquibase

import groovy.transform.CompileStatic
import liquibase.changelog.ChangeLogParameters
import liquibase.changelog.DatabaseChangeLog
import liquibase.parser.ChangeLogParser
import liquibase.parser.ChangeLogParserFactory
import liquibase.resource.ResourceAccessor
import liquibase.serializer.core.string.StringChangeLogSerializer
import org.liquibase.groovy.delegate.DelegateUtil
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.annotations.Scope
import java.util.concurrent.TimeUnit

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@CompileStatic
class BenchmarkTest {
	ResourceAccessor ra = new FolderResourceAccessor(commonTestsChangelogRoot)

	@Benchmark
	 void parseGroovy() {
		DatabaseChangeLog groovy = parseToChangeLog("${commonTestsChangelog}.groovy", ra)
	}

	@Benchmark
	void parseXML() {
		DatabaseChangeLog groovy = parseToChangeLog("${commonTestsChangelog}.xml", ra)
	}

	static final String commonTestsChangelog = 'changelogs/common/test.changelog'
	static final File commonTestsChangelogRoot = new File('D:/dev/liquibase/build4/liquibase-integration-tests/src/test/resources')
	static String serialize(DatabaseChangeLog log) {
		StringChangeLogSerializer serializer = new StringChangeLogSerializer()
		try(PrintWriter p = new PrintWriter(log.filePath+'.txt' )) {
			String r = log.changeSets.inject (new DelegateUtil.CollectionStringBuilder()) { r, c -> r << serializer.serialize(c, false) }.toString()
			p.write(r)
			p.flush()
			return r
		}
	}
	static DatabaseChangeLog parseToChangeLog (String fileName, ResourceAccessor ra,
															 ChangeLogParameters changeLogParameters = new ChangeLogParameters()) {
		ChangeLogParser parser = ChangeLogParserFactory.getInstance().getParser(fileName, ra)
		parser.parse(fileName, changeLogParameters, ra)
	}

	static void main(String[] args) {
		//ResourceAccessor ra = new DirectoryResourceAccessor(Path.of('test'))
		ResourceAccessor ra = new FolderResourceAccessor(commonTestsChangelogRoot)
		//DatabaseChangeLog groovy = parseToChangeLog("changelogs/common/test.groovy", ra)
		DatabaseChangeLog groovy = parseToChangeLog("${commonTestsChangelog}.groovy", ra)
		serialize(groovy)
	}
}

