package sql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/mercury"
	"go.sour.is/pkg/rsql"
	"golang.org/x/exp/maps"
)

type sqlHandler struct {
	db               *sql.DB
	paceholderFormat sq.PlaceholderFormat
	listFormat       [2]rune
}

var (
	_ mercury.GetIndex    = (*sqlHandler)(nil)
	_ mercury.GetConfig   = (*sqlHandler)(nil)
	_ mercury.GetRules    = (*sqlHandler)(nil)
	_ mercury.WriteConfig = (*sqlHandler)(nil)
)

func Register() {
	mercury.Registry.Register("sql", func(s *mercury.Space) any {
		var dsn string
		var opts strings.Builder
		var dbtype string
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
		case "sqlite":
			return &sqlHandler{db, sq.Dollar, [2]rune{'[', ']'}}
		case "postgres":
			return &sqlHandler{db, sq.Dollar, [2]rune{'{', '}'}}
		default:
			return fmt.Errorf("unsupported dbtype: %s", dbtype)
		}
	})
}

type Space struct {
	mercury.Space
	ID uint64
}
type Value struct {
	mercury.Value
	ID uint64
}

func (p *sqlHandler) GetIndex(ctx context.Context, search mercury.NamespaceSearch, pgm *rsql.Program) (mercury.Config, error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	cols := rsql.GetDbColumns(mercury.Space{})
	where, err := getWhere(search, cols)
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

func (p *sqlHandler) GetConfig(ctx context.Context, search mercury.NamespaceSearch, pgm *rsql.Program, fields []string) (mercury.Config, error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	idx, err := p.GetIndex(ctx, search, pgm)
	if err != nil {
		return nil, err
	}
	spaceMap := make(map[string]int, len(idx))
	for u, s := range idx {
		spaceMap[s.Space] = u
	}

	where, err := getWhere(search, rsql.GetDbColumns(mercury.Value{}))
	if err != nil {
		return nil, err
	}
	query := sq.Select(`"space"`, `"name"`, `"seq"`, `"notes"`, `"tags"`, `"values"`).
		From("mercury_registry_vw").
		Where(where).
		OrderBy("space asc", "name asc").
		PlaceholderFormat(p.paceholderFormat)
	span.AddEvent(lg.LogQuery(query.ToSql()))
	rows, err := query.RunWith(p.db).
		QueryContext(ctx)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var s mercury.Value

		err = rows.Scan(
			&s.Space,
			&s.Name,
			&s.Seq,
			listScan(&s.Notes, p.listFormat),
			listScan(&s.Tags, p.listFormat),
			listScan(&s.Values, p.listFormat),
		)
		if err != nil {
			return nil, err
		}
		if u, ok := spaceMap[s.Space]; ok {
			idx[u].List = append(idx[u].List, s)
		}
	}

	err = rows.Err()
	span.RecordError(err)

	span.AddEvent(fmt.Sprint("read index ", len(idx)))
	return idx, err
}

func (p *sqlHandler) listSpace(ctx context.Context, tx sq.BaseRunner, where sq.Sqlizer) ([]*Space, error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	if tx == nil {
		tx = p.db
	}

	query := sq.Select(`"id"`, `"space"`, `"notes"`, `"tags"`, `"trailer"`).
		From("mercury_spaces").
		Where(where).
		OrderBy("space asc").
		PlaceholderFormat(sq.Dollar)
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
			&s.ID,
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
	lis, err := p.listSpace(ctx, tx, sq.Eq{"space": maps.Keys(names)})
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
			deleteIDs = append(deleteIDs, s.ID)
			continue
		}

		updateSpaces = append(updateSpaces, config[names[spaceName]])
		updateIDs = append(updateIDs, s.ID)
	}
	for _, s := range config {
		spaceName := s.Space
		if _, ok := currentNames[spaceName]; !ok {
			insertSpaces = append(insertSpaces, s)
		}
	}

	// delete spaces
	if ids := deleteIDs; len(ids) > 0 {
		_, err = sq.Delete("mercury_spaces").Where(sq.Eq{"id": ids}).RunWith(tx).PlaceholderFormat(sq.Dollar).ExecContext(ctx)
		if err != nil {
			return err
		}
	}

	// delete values
	if ids := append(updateIDs, deleteIDs...); len(ids) > 0 {
		_, err = sq.Delete("mercury_values").Where(sq.Eq{"id": ids}).RunWith(tx).PlaceholderFormat(sq.Dollar).ExecContext(ctx)
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
			PlaceholderFormat(sq.Dollar)
		span.AddEvent(lg.LogQuery(query.ToSql()))
		_, err := query.RunWith(tx).ExecContext(ctx)

		if err != nil {
			return err
		}
		// log.Debugf("UPDATED %d SPACES", len(updateSpaces))
		for _, v := range u.List {
			newValues = append(newValues, &Value{Value: v, ID: updateIDs[i]})
		}
	}

	// insert spaces
	for _, s := range insertSpaces {
		var id uint64
		query := sq.Insert("mercury_spaces").
			PlaceholderFormat(sq.Dollar).
			Columns("space", "tags", "notes", "trailer").
			Values(
				s.Space, 
				listValue(s.Tags, p.listFormat), 
				listValue(s.Notes, p.listFormat),
				listValue(s.Trailer, p.listFormat),
				).
			Suffix("RETURNING \"id\"")
		span.AddEvent(lg.LogQuery(query.ToSql()))

		err := query.
			RunWith(tx).
			QueryRowContext(ctx).
			Scan(&id)
		if err != nil {
			s, v, _ := query.ToSql()
			log.Println(s, v, err)
			return err
		}
		for _, v := range s.List {
			newValues = append(newValues, &Value{Value: v, ID: id})
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
			PlaceholderFormat(sq.Dollar).
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
			s.ID,
			s.Seq,
			s.Name,
			listValue(s.Values, p.listFormat),
			listValue(s.Notes, p.listFormat),
			listValue(s.Tags, p.listFormat),
		)
		// log.Debug(s.Name)

		if i > 0 && i%chunk == 0 {
			// log.Debugf("inserting %v rows into %v", i%chunk, d.Table)
			// log.Debug(insert.ToSql())
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
		// log.Debug(insert.ToSql())
		span.AddEvent(lg.LogQuery(insert.ToSql()))

		_, err = insert.ExecContext(ctx)
		if err != nil {
			// log.Error(err)
			return
		}
	}

	return
}

func getWhere(search mercury.NamespaceSearch, d *rsql.DbColumns) (sq.Sqlizer, error) {
	var where sq.Or
	space, err := d.Col("space")
	if err != nil {
		return nil, err
	}
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
	return where, nil
}
