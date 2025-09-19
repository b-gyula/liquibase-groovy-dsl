package org.liquibase.groovy.delegate
${timestamp}
import groovy.transform.CompileStatic
import groovy.transform.SelfType
import liquibase.database.ColumnParentTypeEnum
import liquibase.database.FkCascadeActionOptions
import static org.liquibase.groovy.delegate.ChangeSetDelegate.*
import static groovy.lang.Closure.DELEGATE_ONLY
<% def skip = ['customChange','createProcedure','sql', 'rollback', 'output', 'dropColumn']
   def skipMapArgVersion = ['createView'] %>
@CompileStatic
@SelfType(ChangeSetDelegate)
trait ChangeSetChildren {
<% methods.findAll{it.args && !skip.contains(it.name) }.each { m -> %>
	${m.fnDef(false, 'addChange', true)}
<% if(m.args.size() > 2) {
%>
	${m.fnDef(true, 'addChange', true)}
<% if(m.childOptional()) {
%>
	${m.fnDef(false, 'addChange')}

	${m.fnDef(true, 'addChange')}
<%  } // Add Map or Map + Closure versions if hasChild
	if(!skipMapArgVersion.contains(m.name) && (m.hasRequired() || m.hasChild )) {
%>
	${m.javadoc(m.args.size() > 3 )}<%
	if (m.hasChild){ %>
	void $m.name(Map<String, Object> params,${m.args.last().toString( true, true)}) {
		addChangeWithChild Tag.$m.name, params, ${m.args.last().name}
	}
<%	} else { %>
	void $m.name(Map<String, Object> params) {
		addMapBasedChange Tag.$m.name, params
	}
<%    }// if(m.hasChild)
    } // map versions
	} //m.args.size() > 2
  } // each %>
}
