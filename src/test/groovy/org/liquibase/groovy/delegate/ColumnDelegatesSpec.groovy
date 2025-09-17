package org.liquibase.groovy.delegate

import liquibase.change.Change
import liquibase.change.core.LoadDataChange
import liquibase.change.core.LoadDataColumnConfig
import liquibase.changelog.ChangeLogParameters
import liquibase.changelog.ChangeSet
import liquibase.changelog.DatabaseChangeLog
import liquibase.statement.DatabaseFunction
import spock.lang.Specification
import org.liquibase.groovy.delegate.ChangeSetDelegate.Tag
import static org.liquibase.groovy.delegate.DelegateUtil.cast

import static groovy.lang.Closure.DELEGATE_FIRST
import static org.liquibase.groovy.delegate.ChangeSetDelegate.Tag.*
import static org.liquibase.groovy.helper.constants.*
import static org.liquibase.groovy.helper.util.assertPropsSet

class ColumnDelegatesSpec extends Specification {
    static <T extends Change> T buildColumnDelegate(Tag change,
                                      @DelegatesTo(strategy = DELEGATE_FIRST) Closure closure) {
        def changelog = new DatabaseChangeLog()
        changelog.changeLogParameters = new ChangeLogParameters()
        ChangeSetDelegate changSet = new ChangeSetDelegate(
                new ChangeSet(changelog)
        )
        changSet.addChangeWithChild(change, [:], closure)
    }

    static <T> T verify( Map<String, Object> expectedProps, List columns, Class<T> cls) {
        1 == columns.size()
        T col = cls.cast(columns[0])
        assertPropsSet expectedProps, col
        col
    }


    final static loadDataColumnType = 'STRING'

    final static expPropsLoadData = [
            name: columnName,
            type: loadDataColumnType,
            defaultValue: 's',
            defaultValueNumeric: 1,
            defaultValueDate: "2010-11-02 07:52:04.0",
            defaultValueBoolean: true,
            defaultValueComputed: new DatabaseFunction("defaultDatabaseValue"),
            header: 'h',
            index: 1,

    ]

    def "loadData with #type arguments" () {
        LoadDataChange ch = buildColumnDelegate loadData,  cl
        expect:
        verify expPropsLoadData, ch.columns, LoadDataColumnConfig
        where:
        type         | cl
        'named'      | { column expPropsLoadData }
        'positional' | {
        expPropsLoadData.with {
            column columnName, loadDataColumnType, defaultValue,
                    defaultValueNumeric, defaultValueDate, defaultValueBoolean,
                    defaultValueComputed, header, index
        }
    }
    }


    final static expPropsLoadUpdateData = [
            name: columnName,
            type: loadDataColumnType,
            defaultValue: 's',
            defaultValueNumeric: 1,
            defaultValueDate: new Date(),
            defaultValueBoolean: true,
            defaultValueComputed: "defaultDatabaseValue",
            header: 'h',
            index: 1,
            allowUpdate: true
    ]

    def "loadUpdateData with #type arguments" () {
        LoadDataChange ch = buildColumnDelegate loadUpdateData, cl
        expect:
        verify expPropsLoadUpdateData, ch.columns, LoadDataColumnConfig
        where:
        type         | cl
        'mixed'      | { expPropsLoadUpdateData.with {
            column columnName, loadDataColumnType, defaultValue,
                    defaultValueNumeric, defaultValueDate, defaultValueBoolean,
                    defaultValueComputed, header, allowUpdate: allowUpdate, index
        }
        }
        'named'      | { column expPropsLoadUpdateData }
        'positional' | { expPropsLoadUpdateData.with {
                column columnName, loadDataColumnType, defaultValue,
                        defaultValueNumeric, defaultValueDate, defaultValueBoolean,
                        defaultValueComputed, header, index, allowUpdate
            }
        }

    }
}
