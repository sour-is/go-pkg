package rsql

import (
	"testing"

	"github.com/matryer/is"
)

func TestReservedToken(t *testing.T) {
	input := `( ) ; , == != ~ < > <= >= [ ]`
	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokLParen, "("},
		{TokRParen, ")"},
		{TokAND, ";"},
		{TokOR, ","},
		{TokEQ, "=="},
		{TokNEQ, "!="},
		{TokLIKE, "~"},
		{TokLT, "<"},
		{TokGT, ">"},
		{TokLE, "<="},
		{TokGE, ">="},
		{TokLBracket, "["},
		{TokRBracket, "]"},
	}

	t.Run("Reserved Tokens", func(t *testing.T) {
		is := is.New(t)

		l := NewLexer(input)

		for _, tt := range tests {
			tok := l.NextToken()
			is.Equal(tt.expectedType, tok.Type)
			is.Equal(tt.expectedLiteral, tok.Literal)
		}
	})
}
func TestNextToken(t *testing.T) {
	input := `director=='name\'s';actor=eq="name's";Year=le=2000,Year>=2010;(one <= -1.0, two != true),three=in=(1,2,3);c4==5`
	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokIdent, `director`},
		{TokEQ, `==`},
		{TokString, `name\'s`},
		{TokAND, `;`},
		{TokIdent, `actor`},
		{TokEQ, `=eq=`},
		{TokString, `name's`},
		{TokAND, `;`},
		{TokIdent, "Year"},
		{TokLE, "=le="},
		{TokInteger, "2000"},
		{TokOR, ","},
		{TokIdent, "Year"},
		{TokGE, ">="},
		{TokInteger, "2010"},
		{TokAND, ";"},
		{TokLParen, "("},
		{TokIdent, "one"},
		{TokLE, "<="},
		{TokFloat, "-1.0"},
		{TokOR, ","},
		{TokIdent, "two"},
		{TokNEQ, "!="},
		{TokTRUE, "true"},
		{TokRParen, ")"},
		{TokOR, ","},
		{TokIdent, "three"},
		{TokExtend, "=in="},
		{TokLParen, "("},
		{TokInteger, "1"},
		{TokOR, ","},
		{TokInteger, "2"},
		{TokOR, ","},
		{TokInteger, "3"},
		{TokRParen, ")"},
		{TokAND, ";"},
		{TokIdent, "c4"},
		{TokEQ, "=="},
		{TokInteger, "5"},
	}

	t.Run("Next Token Parsing", func(t *testing.T) {
		is := is.New(t)

		l := NewLexer(input)

		c := 0
		for _, tt := range tests {
			c++
			tok := l.NextToken()

			is.Equal(tt.expectedType, tok.Type)
			is.Equal(tt.expectedLiteral, tok.Literal)

		}
		is.Equal(c, len(tests))
	})
}
