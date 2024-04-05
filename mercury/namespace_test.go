package mercury_test

import (
	"fmt"
	"testing"

	"github.com/matryer/is"
	"go.sour.is/pkg/mercury"

	sq "github.com/Masterminds/squirrel"
)

func TestNamespaceParse(t *testing.T) {
	var tests = []struct {
		in  string
		out string
		args []any
	}{
		{
			in: "d42.bgp.kapha.*;trace:d42.bgp.kapha",
			out: "(column LIKE ? OR ? LIKE column || '%')",
			args: []any{"d42.bgp.kapha.%", "d42.bgp.kapha"},
		},

		{
			in: "d42.bgp.kapha.*,d42.bgp.kapha",
			out: "(column LIKE ? OR column = ?)",
			args: []any{"d42.bgp.kapha.%", "d42.bgp.kapha"},
		},
	}

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			is := is.New(t)
			out := mercury.ParseNamespace(tt.in)
			sql, args, err := getWhere(out).ToSql()
			is.NoErr(err)
			is.Equal(sql, tt.out)
			is.Equal(args, tt.args)
		})
	}
}

func getWhere(search mercury.NamespaceSearch) sq.Sqlizer {
	var where sq.Or
	space := "column"
	for _, m := range search {
		switch m.(type) {
		case mercury.NamespaceNode:
			where = append(where, sq.Eq{space: m.Value()})
		case mercury.NamespaceStar:
			where = append(where, sq.Like{space: m.Value()})
		case mercury.NamespaceTrace:
			e := sq.Expr(`? LIKE `+space+` || '%'`, m.Value())
			where = append(where, e)
		}
	}
	return where
}
