package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/mercury"
	"golang.org/x/exp/maps"
)

var MAX_FILTER int = 40

type sqlHandler struct {
	name             string
	db               *sql.DB
	paceholderFormat sq.PlaceholderFormat
	listFormat       [2]rune
	readonly         bool
	getWhere         func(search mercury.Search) (func(sq.SelectBuilder) sq.SelectBuilder, error)
}

var (
	_ mercury.GetIndex    = (*sqlHandler)(nil)
	_ mercury.GetConfig   = (*sqlHandler)(nil)
	_ mercury.GetRules    = (*sqlHandler)(nil)
	_ mercury.WriteConfig = (*sqlHandler)(nil)
)

func Register() func(context.Context) error {
	var hdlrs []*sqlHandler
	mercury.Registry.Register("sql", func(s *mercury.Space) any {
		var dsn string
		var opts strings.Builder
		var dbtype string
		var readonly bool = slices.Contains(s.Tags, "readonly")
		for _, c := range s.List {
			if c.Name == "match" {
				continue
			}
			if c.Name == "dbtype" {
				dbtype = c.First()
				continue
			}
			if c.Name == "dsn" {
				dsn = c.First()
				break
			}
			fmt.Fprintln(&opts, c.Name, "=", c.First())
		}
		if dsn == "" {
			dsn = opts.String()
		}
		db, err := openDB(dbtype, dsn)
		if err != nil {
			return err
		}
		if err = db.Ping(); err != nil {
			return err
		}
		switch dbtype {
		case "sqlite", "libsql", "libsql+embed":
			h := &sqlHandler{s.Space, db, sq.Question, [2]rune{'[', ']'}, readonly, GetWhereSQ}
			hdlrs = append(hdlrs, h)
			return h
		case "postgres":
			h := &sqlHandler{s.Space, db, sq.Dollar, [2]rune{'{', '}'}, readonly, GetWherePG}
			hdlrs = append(hdlrs, h)
			return h
		default:
			return fmt.Errorf("unsupported dbtype: %s", dbtype)
		}
	})

	return func(ctx context.Context) error {
		var errs error

		for _, h := range hdlrs {
			// if err = ctx.Err(); err != nil {
			// 	return  errors.Join(errs, err)
			// }
			errs = errors.Join(errs, h.db.Close())
		}

		return errs
	}
}

type Space struct {
	mercury.Space
	id uint64
}
type Value struct {
	mercury.Value
	id uint64
}

func (p *sqlHandler) GetIndex(ctx context.Context, search mercury.Search) (mercury.Config, error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	where, err := p.getWhere(search)
	if err != nil {
		return nil, err
	}
	lis, err := p.listSpace(ctx, nil, where)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	config := make(mercury.Config, len(lis))
	for i, s := range lis {
		config[i] = &s.Space
	}

	return config, nil
}

func (p *sqlHandler) GetConfig(ctx context.Context, search mercury.Search) (config mercury.Config, err error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	where, err := p.getWhere(search)
	if err != nil {
		return nil, err
	}
	lis, err := p.listSpace(ctx, nil, where)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	if len(lis) == 0 {
		return nil, nil
	}

	spaceIDX := make([]uint64, len(lis))
	spaceMap := make(map[uint64]int, len(lis))
	config = make(mercury.Config, len(lis))
	for i, s := range lis {
		spaceIDX[i] = s.id
		config[i] = &s.Space
		spaceMap[s.id] = i
	}

	query := sq.Select(`"id"`, `"name"`, `"seq"`, `"notes"`, `"tags"`, `"values"`).
		From("mercury_values").
		Where(sq.Eq{"id": spaceIDX}).
		OrderBy("id asc", "seq asc").
		PlaceholderFormat(p.paceholderFormat)

	span.AddEvent(p.name)
	span.AddEvent(lg.LogQuery(query.ToSql()))
	rows, err := query.RunWith(p.db).
		QueryContext(ctx)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var s Value

		err = rows.Scan(
			&s.id,
			&s.Name,
			&s.Seq,
			listScan(&s.Notes, p.listFormat),
			listScan(&s.Tags, p.listFormat),
			listScan(&s.Values, p.listFormat),
		)
		if err != nil {
			return nil, err
		}
		if u, ok := spaceMap[s.id]; ok {
			lis[u].List = append(lis[u].List, s.Value)
		}
	}

	err = rows.Err()
	span.RecordError(err)

	span.AddEvent(fmt.Sprint("read index ", len(lis)))
	// log.Println(config.String())
	return config, err
}

func (p *sqlHandler) listSpace(ctx context.Context, tx sq.BaseRunner, where func(sq.SelectBuilder) sq.SelectBuilder) ([]*Space, error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	if tx == nil {
		tx = p.db
	}

	query := sq.Select(`"id"`, `"space"`, `"notes"`, `"tags"`, `"trailer"`).
		From("mercury_spaces").
		OrderBy("space asc").
		PlaceholderFormat(p.paceholderFormat)
	query = where(query)

	span.AddEvent(p.name)
	span.AddEvent(lg.LogQuery(query.ToSql()))
	rows, err := query.RunWith(tx).
		QueryContext(ctx)

	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()

	var lis []*Space
	for rows.Next() {
		var s Space
		err = rows.Scan(
			&s.id,
			&s.Space.Space,
			listScan(&s.Space.Notes, p.listFormat),
			listScan(&s.Space.Tags, p.listFormat),
			listScan(&s.Trailer, p.listFormat),
		)
		if err != nil {
			return nil, err
		}
		lis = append(lis, &s)
	}

	err = rows.Err()
	span.RecordError(err)

	span.AddEvent(fmt.Sprint("read config ", len(lis)))
	return lis, err
}

// WriteConfig writes a config map to database
func (p *sqlHandler) WriteConfig(ctx context.Context, config mercury.Config) (err error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	if p.readonly {
		return fmt.Errorf("readonly database")
	}

	// Delete spaces that are present in input but are empty.
	deleteSpaces := make(map[string]struct{})

	// get names of each space
	var names = make(map[string]int)
	for i, v := range config {
		names[v.Space] = i

		if len(v.Tags) == 0 && len(v.Notes) == 0 && len(v.List) == 0 {
			deleteSpaces[v.Space] = struct{}{}
		}
	}

	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil && tx != nil {
			tx.Rollback()
		}
	}()

	// get current spaces
	where := func(qry sq.SelectBuilder) sq.SelectBuilder { return qry.Where(sq.Eq{"space": maps.Keys(names)}) }
	lis, err := p.listSpace(ctx, tx, where)
	if err != nil {
		return
	}

	// determine which are being updated
	var deleteIDs []uint64
	var updateIDs []uint64
	var currentNames = make(map[string]struct{}, len(lis))
	var updateSpaces []*mercury.Space
	var insertSpaces []*mercury.Space

	for _, s := range lis {
		spaceName := s.Space.Space
		currentNames[spaceName] = struct{}{}

		if _, ok := deleteSpaces[spaceName]; ok {
			deleteIDs = append(deleteIDs, s.id)
			continue
		}

		updateSpaces = append(updateSpaces, config[names[spaceName]])
		updateIDs = append(updateIDs, s.id)
	}
	for _, s := range config {
		spaceName := s.Space
		if _, ok := currentNames[spaceName]; !ok {
			insertSpaces = append(insertSpaces, s)
		}
	}

	// delete spaces
	if ids := deleteIDs; len(ids) > 0 {
		_, err = sq.Delete("mercury_spaces").Where(sq.Eq{"id": ids}).RunWith(tx).PlaceholderFormat(p.paceholderFormat).ExecContext(ctx)
		if err != nil {
			return err
		}
	}

	// delete values
	if ids := append(updateIDs, deleteIDs...); len(ids) > 0 {
		_, err = sq.Delete("mercury_values").Where(sq.Eq{"id": ids}).RunWith(tx).PlaceholderFormat(p.paceholderFormat).ExecContext(ctx)
		if err != nil {
			return err
		}
	}

	var newValues []*Value

	// update spaces
	for i, u := range updateSpaces {
		query := sq.Update("mercury_spaces").
			Where(sq.Eq{"id": updateIDs[i]}).
			Set("tags", listValue(u.Tags, p.listFormat)).
			Set("notes", listValue(u.Notes, p.listFormat)).
			Set("trailer", listValue(u.Trailer, p.listFormat)).
			PlaceholderFormat(p.paceholderFormat)
		span.AddEvent(p.name)
		span.AddEvent(lg.LogQuery(query.ToSql()))
		_, err := query.RunWith(tx).ExecContext(ctx)

		if err != nil {
			return err
		}
		// log.Debugf("UPDATED %d SPACES", len(updateSpaces))
		for _, v := range u.List {
			newValues = append(newValues, &Value{Value: v, id: updateIDs[i]})
		}
	}

	// insert spaces
	for _, s := range insertSpaces {
		var id uint64
		query := sq.Insert("mercury_spaces").
			PlaceholderFormat(p.paceholderFormat).
			Columns("space", "tags", "notes", "trailer").
			Values(
				s.Space,
				listValue(s.Tags, p.listFormat),
				listValue(s.Notes, p.listFormat),
				listValue(s.Trailer, p.listFormat),
			).
			Suffix("RETURNING \"id\"")
		span.AddEvent(p.name)
		span.AddEvent(lg.LogQuery(query.ToSql()))

		err := query.
			RunWith(tx).
			QueryRowContext(ctx).
			Scan(&id)
		if err != nil {
			span.AddEvent(p.name)
			s, v, _ := query.ToSql()
			log.Println(s, v, err)
			return err
		}
		for _, v := range s.List {
			newValues = append(newValues, &Value{Value: v, id: id})
		}
	}

	// write all values to db.
	err = p.writeValues(ctx, tx, newValues)
	// log.Debugf("WROTE %d ATTRS", len(attrs))

	tx.Commit()
	tx = nil

	return
}

// writeValues writes the values to db
func (p *sqlHandler) writeValues(ctx context.Context, tx sq.BaseRunner, lis []*Value) (err error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	if len(lis) == 0 {
		return nil
	}

	newInsert := func() sq.InsertBuilder {
		return sq.Insert("mercury_values").
			RunWith(tx).
			PlaceholderFormat(p.paceholderFormat).
			Columns(
				`"id"`,
				`"seq"`,
				`"name"`,
				`"values"`,
				`"notes"`,
				`"tags"`,
			)
	}
	chunk := int(65000 / 3)
	insert := newInsert()
	for i, s := range lis {
		insert = insert.Values(
			s.id,
			s.Seq,
			s.Name,
			listValue(s.Values, p.listFormat),
			listValue(s.Notes, p.listFormat),
			listValue(s.Tags, p.listFormat),
		)
		// log.Debug(s.Name)

		if i > 0 && i%chunk == 0 {
			// log.Debugf("inserting %v rows into %v", i%chunk, d.Table)
			span.AddEvent(p.name)
			span.AddEvent(lg.LogQuery(insert.ToSql()))

			_, err = insert.ExecContext(ctx)
			if err != nil {
				// log.Error(err)
				return
			}

			insert = newInsert()
		}
	}
	if len(lis)%chunk > 0 {
		// log.Debugf("inserting %v rows into %v", len(lis)%chunk, d.Table)
		span.AddEvent(p.name)
		span.AddEvent(lg.LogQuery(insert.ToSql()))

		_, err = insert.ExecContext(ctx)
		if err != nil {
			// log.Error(err)
			return
		}
	}

	return
}

func GetWherePG(search mercury.Search) (func(sq.SelectBuilder) sq.SelectBuilder, error) {
	var where sq.Or
	space := "space"

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

	var joins []sq.SelectBuilder
	for i, o := range search.Find {
		log.Println(o)
		if i > MAX_FILTER {
			err := fmt.Errorf("too many filters [%d]", MAX_FILTER)
			return nil, err
		}
		q := sq.Select("DISTINCT id").From("mercury_values")

		switch o.Op {
		case "key":
			q = q.Where(sq.Eq{"name": o.Left})
		case "nkey":
			q = q.Where(sq.NotEq{"name": o.Left})
		case "eq":
			q = q.Where("name = ? AND ? = any (values)", o.Left, o.Right)
		case "neq":
			q = q.Where("name = ? AND ? != any (values)", o.Left, o.Right)

		case "gt":
			q = q.Where("name = ? AND ? > any (values)", o.Left, o.Right)
		case "lt":
			q = q.Where("name = ? AND ? < any (values)", o.Left, o.Right)
		case "ge":
			q = q.Where("name = ? AND ? >= any (values)", o.Left, o.Right)
		case "le":
			q = q.Where("name = ? AND ? <= any (values)", o.Left, o.Right)

			// case "like":
			// 	q = q.Where("name = ? AND value LIKE ?", o.Left, o.Right)
			// case "in":
			// 	q = q.Where(sq.Eq{"name": o.Left, "value": strings.Split(o.Right, " ")})
		}
		joins = append(joins, q)
	}

	return func(s sq.SelectBuilder) sq.SelectBuilder {
		for i, q := range joins {
			s = s.JoinClause(q.Prefix("JOIN (").Suffix(fmt.Sprintf(`) r%03d USING (id)`, i)))
		}

		if search.Count > 0 {
			s = s.Limit(search.Count)
		}
		return s.Where(where)
	}, nil
}

func GetWhereSQ(search mercury.Search) (func(sq.SelectBuilder) sq.SelectBuilder, error) {
	var where sq.Or

	var errs error
	id := "id"
	space := "space"
	name := "name"
	values_each := `json_valid("values")`
	values_valid := `json_valid("values")`

	if errs != nil {
		return nil, errs
	}

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

	var joins []sq.SelectBuilder
	for i, o := range search.Find {
		log.Println(o)
		if i > MAX_FILTER {
			err := fmt.Errorf("too many filters [%d]", MAX_FILTER)
			return nil, err
		}
		q := sq.Select("DISTINCT " + id).From(`mercury_values mv, ` + values_each + ` vs`)

		switch o.Op {
		case "key":
			q = q.Where(sq.Eq{name: o.Left})
		case "nkey":
			q = q.Where(sq.NotEq{name: o.Left})
		case "eq":
			q = q.Where(sq.And{sq.Expr(values_valid), sq.Eq{name: o.Left, `vs.value`: o.Right}})
		case "neq":
			q = q.Where(sq.And{sq.Expr(values_valid), sq.Eq{name: o.Left}, sq.NotEq{`vs.value`: o.Right}})

		case "gt":
			q = q.Where(sq.And{sq.Expr(values_valid), sq.Eq{name: o.Left}, sq.Gt{`vs.value`: o.Right}})
		case "lt":
			q = q.Where(sq.And{sq.Expr(values_valid), sq.Eq{name: o.Left}, sq.Lt{`vs.value`: o.Right}})
		case "ge":
			q = q.Where(sq.And{sq.Expr(values_valid), sq.Eq{name: o.Left}, sq.GtOrEq{`vs.value`: o.Right}})
		case "le":
			q = q.Where(sq.And{sq.Expr(values_valid), sq.Eq{name: o.Left}, sq.LtOrEq{`vs.value`: o.Right}})
		case "like":
			q = q.Where(sq.And{sq.Expr(values_valid), sq.Eq{name: o.Left}, sq.Like{`vs.value`: o.Right}})
		case "in":
			q = q.Where(sq.Eq{name: o.Left, "vs.value": strings.Split(o.Right, " ")})
		}
		joins = append(joins, q)
	}

	return func(s sq.SelectBuilder) sq.SelectBuilder {
		for i, q := range joins {
			s = s.JoinClause(q.Prefix("JOIN (").Suffix(fmt.Sprintf(`) r%03d USING (id)`, i)))
		}

		if search.Count > 0 {
			s = s.Limit(search.Count)
		}

		if search.Offset > 0 {
			s = s.Offset(search.Offset)
		}

		return s.Where(where)
	}, nil
}
