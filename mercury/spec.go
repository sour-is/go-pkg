package mercury

import (
	"log"
	"path/filepath"
	"strconv"
	"strings"
)

// Search implements a parsed namespace search
// It parses the input and generates an AST to inform the driver how to select values.
// *                         => all spaces
// mercury.*                 => all prefixed with `mercury.`
// mercury.config            => only space `mercury.config`
// mercury.source.*#readonly => all prefixed with `mercury.source.` AND has tag `readonly`
// test.*|mercury.*          => all prefixed with `test.` AND `mercury.`
// test.* find bin=eq=bar    => all prefixed with `test.` AND has an attribute bin that equals bar
// test.* fields foo,bin     => all prefixed with `test.` only show fields foo and bin
//   - count 20                => start a cursor with 20 results
//   - count 20 after <cursor> => continue after cursor for 20 results
//     cursor encodes start points for each of the matched sources
type Search struct {
	NamespaceSearch
	Find   []ops
	Fields []string
	Count  uint64
	Offset uint64
	Cursor string
}

type NamespaceSpec interface {
	Value() string
	String() string
	Raw() string
	Match(string) bool
}

// NamespaceSearch list of namespace specs
type NamespaceSearch []NamespaceSpec

// ParseNamespace returns a list of parsed values
func ParseSearch(text string) (search Search) {
	ns, text, _ := strings.Cut(text, " ")
	var lis NamespaceSearch
	for _, part := range strings.Split(ns, "|") {
		if strings.HasPrefix(part, "trace:") {
			lis = append(lis, NamespaceTrace(part[6:]))
		} else if strings.Contains(part, "*") {
			lis = append(lis, NamespaceStar(part))
		} else {
			lis = append(lis, NamespaceNode(part))
		}
	}
	search.NamespaceSearch = lis

	field, text, next := strings.Cut(text, " ")
	text = strings.TrimSpace(text)
	for next {
		switch strings.ToLower(field) {
		case "find":
			field, text, _ = strings.Cut(text, " ")
			text = strings.TrimSpace(text)
			search.Find = simpleParse(field)

		case "fields":
			field, text, _ = strings.Cut(text, " ")
			text = strings.TrimSpace(text)
			search.Fields = strings.Split(field, ",")

		case "count":
			field, text, _ = strings.Cut(text, " ")
			text = strings.TrimSpace(text)
			search.Count, _ = strconv.ParseUint(field, 10, 64)

		case "offset":
			field, text, _ = strings.Cut(text, " ")
			text = strings.TrimSpace(text)
			search.Offset, _ = strconv.ParseUint(field, 10, 64)

		case "after":
			field, text, _ = strings.Cut(text, " ")
			text = strings.TrimSpace(text)
			search.Cursor = field
		}
		field, text, next = strings.Cut(text, " ")
		text = strings.TrimSpace(text)
	}

	return
}

// String output string value
func (n NamespaceSearch) String() string {
	lis := make([]string, 0, len(n))

	for _, v := range n {
		lis = append(lis, v.String())
	}
	return strings.Join(lis, ";")
}

// Match returns true if any match.
func (n NamespaceSearch) Match(s string) bool {
	for _, m := range n {
		ok, err := filepath.Match(m.Raw(), s)
		if err != nil {
			return false
		}
		if ok {
			return true
		}
	}

	return false
}

// NamespaceNode implements a node search value
type NamespaceNode string

// String output string value
func (n NamespaceNode) String() string { return string(n) }

// Quote return quoted value.
// func (n NamespaceNode) Quote() string { return `'` + n.Value() + `'` }

// Value to return the value
func (n NamespaceNode) Value() string { return string(n) }

// Raw return raw value.
func (n NamespaceNode) Raw() string { return string(n) }

// Match returns true if any match.
func (n NamespaceNode) Match(s string) bool { return match(n, s) }

// NamespaceTrace implements a trace search value
type NamespaceTrace string

// String output string value
func (n NamespaceTrace) String() string { return "trace:" + string(n) }

// Quote return quoted value.
// func (n NamespaceTrace) Quote() string { return `'` + n.Value() + `'` }

// Value to return the value
func (n NamespaceTrace) Value() string { return strings.Replace(string(n), "*", "%", -1) }

// Raw return raw value.
func (n NamespaceTrace) Raw() string { return string(n) }

// Match returns true if any match.
func (n NamespaceTrace) Match(s string) bool { return match(n, s) }

// NamespaceStar implements a trace search value
type NamespaceStar string

// String output string value
func (n NamespaceStar) String() string { return string(n) }

// Quote return quoted value.
// func (n NamespaceStar) Quote() string { return `'` + n.Value() + `'` }

// Value to return the value
func (n NamespaceStar) Value() string { return strings.Replace(string(n), "*", "%", -1) }

// Raw return raw value.
func (n NamespaceStar) Raw() string { return string(n) }

// Match returns true if any match.
func (n NamespaceStar) Match(s string) bool { return match(n, s) }

func match(n NamespaceSpec, s string) bool {
	ok, err := filepath.Match(n.Raw(), s)
	if err != nil {
		return false
	}
	return ok
}

type ops struct {
	Left  string
	Op    string
	Right string
}

func simpleParse(in string) (out []ops) {
	items := strings.Split(in, ",")
	for _, i := range items {
		log.Println(i)
		eq := strings.Split(i, "=")
		switch len(eq) {
		case 2:
			out = append(out, ops{eq[0], "eq", eq[1]})
		case 3:
			if eq[1] == "" {
				eq[1] = "eq"
			}
			out = append(out, ops{eq[0], eq[1], eq[2]})
		}
	}

	return
}
