package sql

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

type valueFn func() (v driver.Value, err error)

func (fn valueFn) Value() (v driver.Value, err error) {
	return fn()
}

type scanFn func(value any) error

func (fn scanFn) Scan(v any) error {
	return fn(v)
}

func listScan(e *[]string, ends [2]rune) scanFn {
	return func(value any) error {
		var str string
		switch v := value.(type) {
		case string:
			str = v
		case []byte:
			str = string(v)
		case []rune:
			str = string(v)
		default:
			return fmt.Errorf("array must be uint64, got: %T", value)
		}

		if e == nil {
			*e = []string{}
		}

		str = trim(str, ends[0], ends[1])
		if len(str) == 0 {
			return nil
		}

		for _, s := range splitComma(string(str)) {
			*e = append(*e, s)
		}

		return nil
	}
}

func listValue(e []string, ends [2]rune) valueFn {
	return func() (value driver.Value, err error) {
		var b strings.Builder

		if len(e) == 0 {
			return string(ends[:]), nil
		}

		_, err = b.WriteRune(ends[0])
		if err != nil {
			return
		}

		var arr []string
		for _, s := range e {
			arr = append(arr, `"`+s+`"`)
		}
		_, err = b.WriteString(strings.Join(arr, ","))
		if err != nil {
			return
		}

		_, err = b.WriteRune(ends[1])
		if err != nil {
			return
		}

		return b.String(), nil
	}
}

func splitComma(s string) []string {
	lastQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == lastQuote:
			lastQuote = rune(0)
			return false
		case lastQuote != rune(0):
			return false
		case unicode.In(c, unicode.Quotation_Mark):
			lastQuote = c
			return false
		default:
			return c == ','
		}
	}
	lis := strings.FieldsFunc(s, f)

	var out []string
	for _, s := range lis {
		s = trim(s, '"', '"')
		out = append(out, s)
	}

	return out
}

func trim(s string, start, end rune) string {
	r0, size0 := utf8.DecodeRuneInString(s)
	if size0 == 0 {
		return s
	}
	if r0 != start {
		return s
	}

	r1, size1 := utf8.DecodeLastRuneInString(s)
	if size1 == 0 {
		return s
	}
	if r1 != end {
		return s
	}

	return s[size0 : len(s)-size1]
}
