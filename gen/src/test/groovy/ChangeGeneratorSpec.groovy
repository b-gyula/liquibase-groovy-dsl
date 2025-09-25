import groovy.transform.CompileStatic

import static Method.Arg
import static Method.Arg.*
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

	static Arg arg(String name, List<String> required = NO, String type = 'int') {
		new Arg(name, '', required, type)
	}

	void 'argList with optional child'() {
		Method m = Method('fn', '', [arg('a', YES)
											  , arg('b')
											  , arg('c', NO, ClosureType)], true)
		expect:
		m.argList(typeNameNDefault, skipOptionalChild) == expected
		where:
		typeNameNDefault 	| skipOptionalChild | expected
		true  | false| " int a, int b=null, Closure c"
		true  | true | " int a, int b=null"
		false	| true | "('a',a) ('b',b)"
		false	| false| "('a',a) ('b',b) ,c"
	}

	void 'argList without child'() {
		Method m = Method('fn', '', [arg('a', YES)
											  , arg('b')
											  , arg('c', NO, 'String')], false)
		expect:
		m.argList(typeNameNDefault, skipOptionalChild) == expected
		where:
		typeNameNDefault 				  | skipOptionalChild | expected
		true | false | " int a, int b=null, String c=null"
		false	  | false | "('a',a) ('b',b) ('c',c)"
		true | true | " int a, int b=null, String c=null"
		false	  | true | "('a',a) ('b',b) ('c',c)"
	}

	void 'argList with required child'() {
		Method m = Method('fn', '', [arg('a', YES)
											  , arg('b')
											  , arg('c', YES, ClosureType)], true)
		expect:
		m.argList(typeNameNDefault, skipOptionalChild) == expected
		where:
		typeNameNDefault 	| skipOptionalChild | expected
		true 		  | false| " int a, int b=null, Closure c"
		false		  | false| "('a',a) ('b',b) ,c"
		true		  | true | " int a, int b=null, Closure c"
		false		  | true | "('a',a) ('b',b) ,c"
	}

/*	void 'argList with no child'() {
		Method m = Method('fn', '', [arg('a', YES)
											  , arg('b')
											  , arg('c', NO, ClosureType)], false)
		expect:
		m.argList(typeNameNDefault, skipOptionalChild) == expected
		where:
		typeNameNDefault 	| skipOptionalChild | expected
		true  | false| " int a, int b=null, Closure c=null"
		false	| false| " a, b, c"
		true  | true | " int a, int b=null, Closure c=null"
		false	| true | " a, b, c"
	}*/

	void 'args required sort'() {
		expect:
		method.resortArgs().collect { it.name } == expected
		where:
		method	|	expected
		Method('fn', '', [arg('a'), arg('r1', YES), arg('b'), arg('r2', YES)]) | ['r1', 'r2', 'a', 'b']
		Method('fn', '', [arg('a'), arg('r1', YES), arg('b'), arg('cl', YES, ClosureType)]) | ['r1', 'a', 'b', 'cl']
		Method('fn', '', [arg('a'), arg('r1', YES), arg('b'), arg('cl', NO, ClosureType)]) | ['r1', 'a', 'b', 'cl']
	}
	void 'fnDef no desc required child'() {
		Method m = Method('fn', '', [arg('a', YES, 'int')
											  ,arg('b', NO, 'String')
											  ,arg('c', YES, ClosureType)], true)
		expect:
		m.fnDef(addNamedArgs, 'm', skipOptionalChild) == expected
		where:
		addNamedArgs | skipOptionalChild | expected
			false | true| """/**  */
\tvoid fn( int a, String b=null, Closure c) {
\t\tm args(Tag.fn) ('a',a) ('b',b) ,c
\t}"""
		true| true| """/**  */
\tvoid fn( Map<String, Object> namedArgs, int a, String b=null, Closure c) {
\t\tm args(Tag.fn,namedArgs) ('a',a) ('b',b) ,c
\t}"""
	}

	void 'fnDef no desc optional child'() {
		Method m = Method('fn', '', [arg('a', YES, 'int')
											  				  ,arg('b', NO, 'String')
															  ,arg('c', NO, ClosureType)])
		expect:
		m.fnDef(false, 'mt', true) == """/**  */
\tvoid fn( int a, String b=null, Closure c=null) {
\t\tmt Tag.fn, a, b, c
\t}"""
	}

	void 'fnDef docs as plain text'() {
		Method m = Method('fn', 'fn desc', [new Arg('a', 'desc_a', YES, 'int')
														, new Arg('b', 'desc b', NO, 'String')
														, new Arg('c', 'desc c', NO, ClosureType)])
		expect:
		m.fnDef(false, 'mt', true) == """/** fn desc
\t  @param a desc_a
\t  @param b desc b
\t  @param c desc c */
\tvoid fn( int a, String b=null, Closure c=null) {
\t\tmt Tag.fn, a, b, c
\t}"""
	}

	void 'fnDef html desc'() {
		Method m = Method('fn', 'fn desc', [new Arg('a', 'desc_a', YES, 'int')
														, new Arg('b2', 'desc b2', NO, 'String')
														, new Arg('b1', 'desc b1', YES, 'String')
														, new Arg('c', 'desc c', NO, ClosureType)])
		m.resortArgs()
		expect:
		m.fnDef(false, 'mt', true) == """/** fn desc
	 <br>Params:<dl>
	 <dt><b>a</b></dt>
		<dd>desc_a</dd>
	 <dt><b>b1</b></dt>
		<dd>desc b1</dd>
	 <dt>b2</dt>
		<dd>desc b2</dd>
	 <dt>c</dt>
		<dd>desc c</dd>
	</dl> */
	void fn( int a, String b1, String b2=null, Closure c=null) {
		mt Tag.fn, a, b1, b2, c
	}"""
	}

	void 'arg required #req.requiredExcept' () {
		expect:
		req.required == expected

		where:
		req | expected
		arg('required', YES) | true
		arg('not required', NO) | false
		arg('except required', ['c']) | false
		arg('except required', ['']) | false
	}
}
