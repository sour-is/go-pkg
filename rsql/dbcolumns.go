package rsql

import (
	"fmt"
	"reflect"
	"strings"
	"unicode"
)

// DbColumns database model metadata
type DbColumns struct {
	Cols  []string
	index map[string]int
	Table string
	View  string
}

// Col returns the mapped column names
func (d *DbColumns) Col(column string) (s string, err error) {
	idx, ok := d.Index(column)
	if !ok {
		err = fmt.Errorf("column not found on table: %v", column)
		return
	}
	return d.Cols[idx], err
}

// Index returns the column number
func (d *DbColumns) Index(column string) (idx int, ok bool) {
	idx, ok = d.index[column]
	return
}

// GetDbColumns builds a metadata struct
func GetDbColumns(o interface{}) *DbColumns {
	d := DbColumns{}
	t := reflect.TypeOf(o)

	d.index = make(map[string]int)
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		tag, _, _ := strings.Cut(field.Tag.Get("db"), ",")

		json := field.Tag.Get("json")
		json, _, _ = strings.Cut(json, ",")
		if tag == "" {
			tag = json
		}

		graphql := field.Tag.Get("graphql")
		graphql, _, _ = strings.Cut(graphql, ",")
		if tag == "" {
			tag = graphql
		}

		if tag == "-" {
			continue
		}

		if tag == "" {
			tag = field.Name
		}

		d.index[field.Name] = len(d.Cols)

		if _, ok := d.index[tag]; !ok && tag != "" {
			d.index[tag] = len(d.Cols)
		}
		if _, ok := d.index[json]; !ok && json != "" {
			d.index[json] = len(d.Cols)
		}
		if _, ok := d.index[graphql]; !ok && graphql != "" {
			d.index[graphql] = len(d.Cols)
		} else if !ok && graphql == "" {
			a := []rune(field.Name)
			for i := 0; i < len(a); i++ {
				if unicode.IsLower(a[i]) {
					break
				}
				a[i] = unicode.ToLower(a[i])
			}
			graphql = string(a)
			d.index[graphql] = len(d.Cols)
		}

		d.Cols = append(d.Cols, tag)
	}
	return &d
}

func QuoteCols(cols []string) []string {
	lis := make([]string, len(cols))
	for i := range cols {
		lis[i] = `"` + cols[i] + `"`
	}
	return lis
}
