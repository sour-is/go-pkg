package squirrel

import (
	"fmt"
	"testing"

	"github.com/Masterminds/squirrel"
	"github.com/matryer/is"
	"go.sour.is/pkg/rsql"
)

type testTable struct {
	Foo      string `json:"foo"`
	Bar      string `json:"bar"`
	Baz      string `json:"baz"`
	Director string `json:"director"`
	Actor    string `json:"actor"`
	Year     string `json:"year"`
	Genres   string `json:"genres"`
	One      string `json:"one"`
	Two      string `json:"two"`
	Family   string `json:"family_name"`
}

func TestQuery(t *testing.T) {
	d := rsql.GetDbColumns(testTable{})
	is := is.New(t)

	tests := []struct {
		input        string
		expect       squirrel.Sqlizer
		expectLimit  *uint64
		expectOffset *uint64
		expectOrder  []string
		fail         bool
	}{
		{input: "foo==[1, 2, 3]", expect: squirrel.Eq{"foo": []interface{}{1, 2, 3}}},
		{input: "foo==1,(bar==2;baz==3)", expect: squirrel.Or{squirrel.Eq{"foo": 1}, squirrel.And{squirrel.Eq{"bar": 2}, squirrel.Eq{"baz": 3}}}},

		{input: "foo==1", expect: squirrel.Eq{"foo": 1}},
		{input: "foo!=1.1", expect: squirrel.NotEq{"foo": 1.1}},
		{input: "foo==true", expect: squirrel.Eq{"foo": true}},
		{input: "foo==null", expect: squirrel.Eq{"foo": nil}},
		{input: "foo>2", expect: squirrel.Gt{"foo": 2}},
		{input: "foo>=2.1", expect: squirrel.GtOrEq{"foo": 2.1}},
		{input: "foo<3", expect: squirrel.Lt{"foo": 3}},
		{input: "foo<=3.1", expect: squirrel.LtOrEq{"foo": 3.1}},

		{input: "foo=eq=1", expect: squirrel.Eq{"foo": 1}},
		{input: "foo=neq=1.1", expect: squirrel.NotEq{"foo": 1.1}},
		{input: "foo=gt=2", expect: squirrel.Gt{"foo": 2}},
		{input: "foo=ge=2.1", expect: squirrel.GtOrEq{"foo": 2.1}},
		{input: "foo=lt=3", expect: squirrel.Lt{"foo": 3}},
		{input: "foo=le=3.1", expect: squirrel.LtOrEq{"foo": 3.1}},

		{input: "foo==1;bar==2", expect: squirrel.And{squirrel.Eq{"foo": 1}, squirrel.Eq{"bar": 2}}},
		{input: "foo==1,bar==2", expect: squirrel.Or{squirrel.Eq{"foo": 1}, squirrel.Eq{"bar": 2}}},
		{input: "foo==1,bar==2;baz=3", expect: nil},
		{
			input: `director=='name\'s';actor=eq="name\'s";Year=le=2000,Year>=2010;one <= -1.0, two != true`,
			expect: squirrel.And{
				squirrel.Eq{"director": "name's"},
				squirrel.Eq{"actor": "name's"},
				squirrel.Or{
					squirrel.LtOrEq{"year": 2000},
					squirrel.GtOrEq{"year": 2010},
				},
				squirrel.Or{
					squirrel.LtOrEq{"one": -1.0},
					squirrel.NotEq{"two": true},
				},
			},
		},
		{
			input: `genres==[sci-fi,action] ; genres==[romance,animated,horror] , director~Que*Tarantino`,
			expect: squirrel.And{
				squirrel.Eq{"genres": []interface{}{"sci-fi", "action"}},
				squirrel.Or{
					squirrel.Eq{"genres": []interface{}{"romance", "animated", "horror"}},
					Like{"director", "Que%Tarantino"},
				},
			},
		},
		{input: "", expect: nil},
		{input: "family_name==LUNDY", expect: squirrel.Eq{"family_name": "LUNDY"}},
		{input: "family_name==[1,2,null]", expect: squirrel.Eq{"family_name": []interface{}{1, 2, nil}}},
		{input: "family_name=LUNDY", expect: nil},
		{input: "family_name==LUNDY and family_name==SMITH", expect: squirrel.And{squirrel.Eq{"family_name": "LUNDY"}, squirrel.Eq{"family_name": "SMITH"}}},
		{input: "family_name==LUNDY or family_name==SMITH", expect: squirrel.Or{squirrel.Eq{"family_name": "LUNDY"}, squirrel.Eq{"family_name": "SMITH"}}},
		{input: "foo==1,family_name=LUNDY;baz==2", expect: nil},
		{input: "foo ~ bar*", expect: Like{"foo", "bar%"}},
		{input: "foo ~ [bar*,bin*]", expect: nil, fail: true},
		{input: "foo==1|limit:10", expect: squirrel.Eq{"foo": 1}, expectLimit: ptr(uint64(10))},
		{input: "foo==1|offset:2", expect: squirrel.Eq{"foo": 1}, expectOffset: ptr(uint64(2))},
		{
			input: "foo>=1|limit:10 offset:2 foo:desc", 
			expect: squirrel.GtOrEq{"foo": 1}, 
			expectLimit: ptr(uint64(10)),
			expectOffset: ptr(uint64(2)),
			expectOrder: []string{"foo desc"},
		},
	}

	for i, tt := range tests {
		q, a, err := Query(tt.input, d)
		if !tt.fail && err != nil {
			t.Error(err)
		}
		if q != nil {
			t.Log(q.ToSql())
		}

		actual := fmt.Sprintf("%#v", q)
		expect := fmt.Sprintf("%#v", tt.expect)
		if expect != actual {
			t.Errorf("test[%d]: %v\n\tinput and expected are not the same. wanted=%s got=%s", i, tt.input, expect, actual)
		}

		if limit, ok := a.Limit(); tt.expectLimit != nil {
			is.True(ok)
			is.Equal(limit, *tt.expectLimit)
		} else {
			is.True(!ok)
		}

		if offset, ok := a.Offset(); tt.expectOffset != nil {
			is.True(ok)
			is.Equal(offset, *tt.expectOffset)
		} else {
			is.True(!ok)
		}

		if order := a.Order(); tt.expectOrder != nil {
			is.Equal(order, tt.expectOrder)
		} else {
			is.Equal(len(order), 0)
		}
	}
}

func ptr[T any](t T) *T { return &t }
