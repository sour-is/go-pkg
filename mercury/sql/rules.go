package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/Masterminds/squirrel"
	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/mercury"
	"go.sour.is/pkg/ident"
)

type grouper interface {
	GetGroups() []string
}

// GetRules get list of rules
func (p *sqlHandler) GetRules(ctx context.Context, user ident.Ident) (lis mercury.Rules, err error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	var ids []string
	ids = append(ids, "U-"+user.Identity())
	switch u := user.(type) {
	case grouper:
		for _, g := range u.GetGroups() {
			ids = append(ids, "G-"+g)
		}
	}
	if groups, err := p.getGroups(ctx, user.Identity()); err != nil {
		for _, g := range groups {
			ids = append(ids, "G-"+g)
		}
	}

	query := squirrel.Select(`"role"`, `"type"`, `"match"`, `"rule"`).
		From("mercury_rules_vw").
		Where(squirrel.Eq{"id": ids}).
		PlaceholderFormat(squirrel.Dollar)
	rows, err := query.
		RunWith(p.db).
		QueryContext(ctx)

	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var s mercury.Rule
		var rule string
		err = rows.Scan(&s.Role, &s.Type, &s.Match, &rule)
		if err != nil {
			span.RecordError(err)
			return nil, err
		}
		if rule != "" {
			s.Role, rule, _ = strings.Cut(rule, " ")
			s.Type, s.Match, _ = strings.Cut(rule, " ")
		}
		lis = append(lis, s)
	}
	err = rows.Err()
	span.RecordError(err)

	span.AddEvent(fmt.Sprint("read rules ", len(lis)))
	return lis, err
}

// getGroups get list of groups
func (pgm *sqlHandler) getGroups(ctx context.Context, user string) (lis []string, err error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	rows, err := squirrel.Select("group_id").
		From("mercury_groups_vw").
		Where(squirrel.Eq{"user_id": user}).
		PlaceholderFormat(squirrel.Dollar).
		RunWith(pgm.db).
		QueryContext(ctx)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var s string
		err = rows.Scan(&s)
		if err != nil {
			return nil, err
		}
		lis = append(lis, s)
	}

	return lis, rows.Err()
}
