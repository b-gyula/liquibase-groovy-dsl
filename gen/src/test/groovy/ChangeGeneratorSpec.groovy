import groovy.transform.CompileStatic

import static Method.Arg
import spock.lang.*

//@CompileStatic
@Newify([Method, Arg])
class ChangeGeneratorSpec extends Specification {
//    static void main(String[] args) {
//        argListWithOptionalChild()
//    }
//    void "Arg.desc cleanup" () {
//        Arg arg = new Arg(desc: input)
//
//        expect:
//        arg.desc == expDesc
//        where:
//        input                | expDesc
//        "  |  \n   as  ldas " | "|  \n\t\tas  ldas"
//        "e @param s"    | "e {@code param} s"
//        " asd*/"           | "asd*\\/"
//    }

    static Arg arg(String name, boolean required = false, String type = 'int') {
        new Arg(name, '', required, type )
    }

    void 'argList with optional child'() {
        Method m = Method('fn', '', [ arg('a',true )
                                  ,arg('b')
                                  ,arg('c',  false,'Closure')], true)
        expect:
        " int a, int b=null, Closure c" == m.argList(true )
        " a, b, c" == m.argList()
        " int a, int b=null" == m.argList(true, true)
        " a, b"== m.argList(false, true )
    }

    void 'argList without child'() {
        Method m = Method('fn', '', [ arg('a',true )
                                      ,arg('b')
                                      ,arg('c',  false,'Closure')], false)
        expect:
        " int a, int b=null, Closure c=null" == m.argList(true )
        " a, b, c" == m.argList()
        " int a, int b=null, Closure c=null" == m.argList(true,true )
        " a, b, c" == m.argList(false, true )
    }

    void 'argList with required child'() {
        Method m = Method('fn', '', [ arg('a',true )
                                      ,arg('b')
                                      ,arg('c',  true,'Closure')] , true)
        expect:
        " int a, int b=null, Closure c" == m.argList(true )
        " a, b, c" == m.argList()
        " int a, int b=null, Closure c" == m.argList(true,true )
        " a, b, c" == m.argList(false, true )
    }

    void 'argList with no child'() {
        Method m = Method('fn', '', [ arg('a',true )
                                      ,arg('b')
                                      ,arg('c',  false,'Closure')] , false)
        expect:
        " int a, int b=null, Closure c=null" == m.argList(true )
        " a, b, c" == m.argList()
        " int a, int b=null, Closure c=null" == m.argList(true,true )
        " a, b, c" == m.argList(false, true )
    }
}
