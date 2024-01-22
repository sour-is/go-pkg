package mercury

import (
	"path/filepath"
	"strings"
)

// NamespaceSpec implements a parsed namespace search
type NamespaceSpec interface {
	Value() string
	String() string
	Raw() string
	Match(string) bool
}

// NamespaceSearch list of namespace specs
type NamespaceSearch []NamespaceSpec

// ParseNamespace returns a list of parsed values
func ParseNamespace(ns string) (lis NamespaceSearch) {
	for _, part := range strings.Split(ns, ";") {
		if strings.HasPrefix(part, "trace:") {
			for _, s := range strings.Split(part[6:], ",") {
				lis = append(lis, NamespaceTrace(s))
			}
		} else {
			for _, s := range strings.Split(part, ",") {
				if strings.Contains(s, "*") {
					lis = append(lis, NamespaceStar(s))
				} else {
					lis = append(lis, NamespaceNode(s))
				}
			}
		}
	}

	return
}

// String output string value
func (n NamespaceSearch) String() string {
	lis := make([]string, 0, len(n))

	for _, v := range n {
		lis = append(lis, v.String())
	}
	return strings.Join(lis, ",")
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
