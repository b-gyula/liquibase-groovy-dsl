package org.liquibase.groovy.delegate
${timestamp}
import groovy.transform.CompileStatic
import groovy.transform.SelfType
import liquibase.database.ObjectQuotingStrategy
import static org.liquibase.groovy.delegate.PreconditionDelegate.Tag
<% def skip = ['customPrecondition'] %>
@CompileStatic
@SelfType(PreconditionDelegate)
trait PreConditionChildren {
<% methods.findAll{it.args && !it.hasChild && !skip.contains(it.name) }.each { m -> %>
	${m.functionDefinitions('addPrecondition', true, '')}
<%	} // each %>
}
