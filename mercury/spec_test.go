package mercury_test

import (
	"fmt"
	"testing"

	"github.com/matryer/is"
	"go.sour.is/pkg/mercury"
	"go.sour.is/pkg/mercury/sql"

	sq "github.com/Masterminds/squirrel"
)

var MAX_FILTER int = 40

func TestNamespaceParse(t *testing.T) {
	var tests = []struct {
		getWhere func(mercury.Search) sq.Sqlizer
		in       string
		out      string
		args     []any
	}{
		{
			getWhere: getWhere,
			in:       "d42.bgp.kapha.*|trace:d42.bgp.kapha",
			out:      "(column LIKE ? OR ? LIKE column || '%')",
			args:     []any{"d42.bgp.kapha.%", "d42.bgp.kapha"},
		},

		{
			getWhere: getWhere,
			in:       "d42.bgp.kapha.*|d42.bgp.kapha",
			out:      "(column LIKE ? OR column = ?)",
			args:     []any{"d42.bgp.kapha.%", "d42.bgp.kapha"},
		},

		{
			getWhere: mkWhere(t, sql.GetWhereSQ),
			in:       "d42.bgp.kapha.* find active=eq=true",
			out:      `SELECT * FROM spaces JOIN ( SELECT DISTINCT id FROM mercury_values mv, json_each(mv."values") vs WHERE (json_valid("values") AND name = ? AND vs.value = ?) ) r000 USING (id) WHERE (space LIKE ?)`,
			args:     []any{"active", "true", "d42.bgp.kapha.%"},
		},

		{
			getWhere: mkWhere(t, sql.GetWhereSQ),
			in:       "d42.bgp.kapha.*   count 10 offset 5",
			out:      `SELECT * FROM spaces WHERE (space LIKE ?) LIMIT 10 OFFSET 5`,
			args:     []any{"d42.bgp.kapha.%"},
		},

		{
			getWhere: mkWhere(t, sql.GetWhereSQ),
			in:       "d42.bgp.kapha.* fields a,b,c",
			out:      `SELECT * FROM spaces WHERE (space LIKE ?)`,
			args:     []any{"d42.bgp.kapha.%"},
		},

		{
			getWhere: mkWhere(t, sql.GetWhereSQ),
			in:       "dn42.* find @type=in=[person,net]",
			out:      `SELECT `,
			args:     []any{"d42.bgp.kapha.%"},
		},
	}

//SELECT * FROM spaces JOIN ( SELECT DISTINCT id FROM mercury_values mv, json_valid("values") vs WHERE (json_valid("values") AND name = ? AND vs.value = ?) ) r000 USING (id) WHERE (space LIKE ?) != 
//SELECT * FROM spaces JOIN ( SELECT DISTINCT mv.id FROM mercury_values mv, json_each(mv."values") vs WHERE (json_valid("values") AND name = ? AND vs.value = ?) ) r000 USING (id) WHERE (space LIKE ?)

	for i, tt := range tests {
		t.Run(fmt.Sprintf("test %d", i), func(t *testing.T) {
			is := is.New(t)
			out := mercury.ParseSearch(tt.in)
			sql, args, err := tt.getWhere(out).ToSql()
			is.NoErr(err)
			is.Equal(sql, tt.out)
			is.Equal(args, tt.args)
		})
	}
}

func getWhere(search mercury.Search) sq.Sqlizer {
	var where sq.Or
	space := "column"
	for _, m := range search.NamespaceSearch {
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

func mkWhere(t *testing.T, where func(search mercury.Search) (func(sq.SelectBuilder) sq.SelectBuilder, error)) func(search mercury.Search) sq.Sqlizer {
	t.Helper()

	return func(search mercury.Search) sq.Sqlizer {
		w, err := where(search)
		if err != nil {
			t.Log(err)
			t.Fail()
		}
		return w(sq.Select("*").From("spaces"))
	}
}
