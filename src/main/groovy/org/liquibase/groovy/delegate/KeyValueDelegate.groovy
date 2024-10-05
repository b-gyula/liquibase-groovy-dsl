/*
 * Copyright 2011-2024 Tim Berglund and Steven C. Saliman
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.liquibase.groovy.delegate

import liquibase.exception.ChangeLogParseException

/**
 * A general-purpose delegate class to provide key/value support in a builder.  This delegate
 * supports 2 ways of creating the key/value pairs.  We can pass them in a manner consistent with
 * the XML, namely a series of {@code param ( name : ' someName ' , value : ' someValue ' ) )
 * elements.  The Groovy DSL parser also supports a simpler mechanism whereby any method in the
 * closure is assumed to be the key and the method's arguments are assumed to be the value.  So the
 * code snippet above becomes {@code someName ( ' someValue ' )}*
 * <p>
 * The map created by this delegate will not do database changeLog property  substitution, that will
 * be up to the caller.
 *
 * Used by both customPrecondition and customChange
 *
 * @author Steven C. Saliman
 */
@groovy.transform.CompileStatic
class KeyValueDelegate extends Delegatee {
    final Map<String, Object> map = [:]
    protected final String parentName
    KeyValueDelegate(String parentName, String changeSetId){
        super(null, changeSetId)
        this.parentName = parentName
    }

    /**
     * This method supports the standard XML like method of passing a name/value pair inside a
     * {@code param} method
     * @param params
     */
    void param(Map<String, Object> params) {
        String mapKey = null
        def mapValue = null
        params.each { key, value ->
            if ( key == "name" ) {
                mapKey = value
            } else if ( key == "value" ) {
                mapValue = value
            } else {
                throw new ChangeLogParseException("'$changeSetId': '${key}' is an invalid property for '$parentName' parameters.")
            }
        }

        // we don't need a value, but we do need a key
        if ( mapKey == null ) {
            throw new ChangeLogParseException("'${changeSetId}': '$parentName' parameters need at least a name.")
        }
        map[mapKey] = mapValue
    }

    /**
     * This method supports the Groovy DSL mechanism of passing a name/value pair by using the
     * method name as the key and the method arguments as the value.
     * @param name
     * @param args
     */
    void methodMissing(String name, args) {
        if ( args != null && args instanceof Object[]) {
            if(args.size() == 1 ) {
                map[name] = args[0]
            }
        } else {
            map[name] = args
        }
    }
}

