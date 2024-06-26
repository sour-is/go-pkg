package rsql

import (
	"fmt"
	"strings"
	"testing"

	"github.com/matryer/is"
)

func typeOf(t any) string { return fmt.Sprintf("%T", t) }

func TestIdentifierExpression(t *testing.T) {
	input := `foobar`

	t.Run("Identifier Expressions", func(t *testing.T) {
		is := is.New(t)

		l := NewLexer(input)
		p := NewParser(l)

		program := p.ParseProgram(nil)
		checkParserErrors(t, p)

		is.Equal(len(program.Statements), 1)
		// if len(program.Statements) != 1 {
		// 	t.Fatalf("program has not envough statements. got=%d", len(program.Statements))
		// }
	})
}

func TestIntegerExpression(t *testing.T) {
	input := `5`

	t.Run("IntegerExpression", func(t *testing.T) {
		is := is.New(t)

		l := NewLexer(input)
		p := NewParser(l)

		program := p.ParseProgram(nil)
		checkParserErrors(t, p)

		is.Equal(len(program.Statements), 1)
		// if len(program.Statements) != 1 {
		// 	t.Fatalf("program has not enough statements. got=%d", len(program.Statements))
		// }

		stmt, ok := program.Statements[0].(*ExpressionStatement)
		is.Equal(typeOf(program.Statements[0]), typeOf(&ExpressionStatement{}))
		is.True(ok)
		// if !ok {
		// 	t.Fatalf("program.Statements[0] is not ExpressionStatement got=%T",
		// 		program.Statements[0])
		// }

		literal, ok := stmt.Expression.(*Integer)
		is.Equal(typeOf(literal), typeOf(&Integer{}))
		is.True(ok)
		// if !ok {
		// 	t.Fatalf("stmt.Expression is not Integer got=%T",
		// 		stmt.Expression)
		// }

		is.Equal(literal.Value, int64(5))
		// if literal.Value != 5 {
		// 	t.Errorf("literal.Value not %d. got=%d", 5, literal.Value)
		// }

		is.Equal(literal.TokenLiteral(), "5")
		// if literal.TokenLiteral() != "5" {
		// 	t.Errorf("literal.TokenLiteral not %v. got=%v", "5", literal.TokenLiteral())
		// }
	})
}

func TestInfixExpression(t *testing.T) {
	tests := []struct {
		input    string
		left     string
		operator string
		right    int64
	}{
		{"foo == 1", "foo", "==", 1},
		{"bar >  2", "bar", ">", 2},
		{"bin <  3", "bin", "<", 3},
		{"baz != 4", "baz", "!=", 4},
		{"buf >= 5", "buf", ">=", 5},
		{"goz <= 6", "goz", "<=", 6},
	}
	t.Run("Infix Expressions", func(t *testing.T) {
		is := is.New(t)

		for _, tt := range tests {
			l := NewLexer(tt.input)
			p := NewParser(l)
			program := p.ParseProgram(nil)
			checkParserErrors(t, p)

			is.Equal(len(program.Statements), 1)
			// if len(program.Statements) != 1 {
			// 	t.Fatalf("program has not envough statements. got=%d", len(program.Statements))
			// }

			stmt, ok := program.Statements[0].(*ExpressionStatement)
			is.Equal(typeOf(stmt), typeOf(&ExpressionStatement{}))
			is.True(ok)
			// if !ok {
			// 	t.Fatalf("program.Statements[0] is not ExpressionStatement got=%T",
			// 		program.Statements[0])
			// }

			exp, ok := stmt.Expression.(*InfixExpression)
			is.Equal(typeOf(exp), typeOf(&InfixExpression{}))
			is.True(ok)
			// if !ok {
			// 	t.Fatalf("stmt.Expression is not InfixExpression got=%T",
			// 		stmt.Expression)
			// }

			if !testIdentifier(t, exp.Left, tt.left) {
				return
			}

			is.Equal(exp.Operator, tt.operator)
			// if exp.Operator != tt.operator {
			// 	t.Fatalf("exp.Operator is not '%v'. got '%v'", tt.operator, exp.Operator)
			// }

			if testInteger(t, exp.Right, tt.right) {
				return
			}
		}
	})
}

func TestOperatorPrecedenceParsing(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{
			"foo == 1; bar == 2.0",
			"((foo==1);(bar==2.0))",
		},
		{
			`director=='name\'s';actor=eq="name\'s";Year=le=2000,Year>=2010;one <= -1.0, two != true`,
			`((((director=="name's");(actor=eq="name's"));((Year=le=2000),(Year>=2010)));((one<=-1.0),(two!=true)))`,
		},
	}
	t.Run("Operator Precidence Parsing", func(t *testing.T) {
		is := is.New(t)

		for _, tt := range tests {
			l := NewLexer(tt.input)
			p := NewParser(l)
			program := p.ParseProgram(nil)
			checkParserErrors(t, p)

			actual := program.String()
			is.Equal(actual, tt.expect)
			// if actual != tt.expect {
			// 	t.Errorf("expcected=%q, got=%q", tt.expect, actual)
			// }
		}
	})
}

func TestParsingArray(t *testing.T) {
	input := "[1, 2.1, true, null]"

	l := NewLexer(input)
	p := NewParser(l)
	program := p.ParseProgram(nil)
	checkParserErrors(t, p)

	if len(program.Statements) != 1 {
		t.Fatalf("program has not enough statements. got=%d", len(program.Statements))
	}

	stmt, ok := program.Statements[0].(*ExpressionStatement)
	if !ok {
		t.Fatalf("program.Statements[0] is not ExpressionStatement got=%T",
			program.Statements[0])
	}

	array, ok := stmt.Expression.(*Array)
	if !ok {
		t.Fatalf("stmt.Expression is not Array got=%T",
			stmt.Expression)
	}

	if len(array.Elements) != 4 {
		t.Fatalf("len(array.Elements) not 4. got=%v", len(array.Elements))
	}

	testInteger(t, array.Elements[0], 1)
	testFloat(t, array.Elements[1], 2.1)
	testBool(t, array.Elements[2], true)
	testNull(t, array.Elements[3])
}

func checkParserErrors(t *testing.T, p *Parser) {
	errors := p.Errors()
	if len(errors) == 0 {
		return
	}

	t.Errorf("parser has %d errors", len(errors))
	for _, msg := range errors {
		t.Errorf("parser error: %q", msg)
	}
	t.FailNow()
}

func testInteger(t *testing.T, e Expression, value int64) bool {
	literal, ok := e.(*Integer)
	if !ok {
		t.Errorf("stmt.Expression is not Integer got=%T", e)
		return false
	}

	if literal.Value != value {
		t.Errorf("literal.Value not %d. got=%d", value, literal.Value)
		return false
	}

	if literal.TokenLiteral() != fmt.Sprintf("%v", value) {
		t.Errorf("literal.TokenLiteral not %v. got=%v", value, literal.TokenLiteral())
		return false
	}

	return true
}
func testFloat(t *testing.T, e Expression, value float64) bool {
	literal, ok := e.(*Float)
	if !ok {
		t.Errorf("stmt.Expression is not Float got=%T", e)
		return false
	}

	if literal.Value != value {
		t.Errorf("literal.Value not %f. got=%f", value, literal.Value)
		return false
	}

	if literal.TokenLiteral() != fmt.Sprintf("%v", value) {
		t.Errorf("literal.TokenLiteral not %q. got=%q", fmt.Sprintf("%v", value), literal.TokenLiteral())
		return false
	}

	return true
}
func testBool(t *testing.T, e Expression, value bool) bool {
	literal, ok := e.(*Bool)
	if !ok {
		t.Errorf("stmt.Expression is not Float got=%T", e)
		return false
	}

	if literal.Value != value {
		t.Errorf("literal.Value not %t. got=%t", value, literal.Value)
		return false
	}

	if literal.TokenLiteral() != fmt.Sprintf("%v", value) {
		t.Errorf("literal.TokenLiteral not %v. got=%v", value, literal.TokenLiteral())
		return false
	}

	return true
}
func testNull(t *testing.T, e Expression) bool {
	literal, ok := e.(*Null)
	if !ok {
		t.Errorf("stmt.Expression is not Null got=%T", e)
		return false
	}
	if literal.Token.Type != TokNULL {
		t.Errorf("liternal.Token is not TokNULL got=%T", e)
		return false
	}

	return true
}
func testIdentifier(t *testing.T, e Expression, value string) bool {
	literal, ok := e.(*Identifier)
	if !ok {
		t.Errorf("stmt.Expression is not Integer got=%T", e)
		return false
	}

	if literal.Value != value {
		t.Errorf("literal.Value not %s. got=%s", value, literal.Value)
		return false
	}

	if literal.TokenLiteral() != value {
		t.Errorf("literal.TokenLiteral not %v. got=%v", value, literal.TokenLiteral())
		return false
	}

	return true
}

func TestReplace(t *testing.T) {
	is := is.New(t)
	is.Equal(strings.Replace(`name\'s`, `\'`, `'`, -1), `name's`)
}
