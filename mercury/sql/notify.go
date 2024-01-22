package sql

import (
	"context"
	"strings"

	"github.com/Masterminds/squirrel"

	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/mercury"
)

// Notify stores the attributes for a registry space
type Notify struct {
	Name   string `json:"name" view:"mercury_notify_vw"`
	Match  string `json:"match"`
	Event  string `json:"event"`
	Method string `json:"-" db:"method"`
	URL    string `json:"-" db:"url"`
}

// GetNotify get list of rules
func (pgm *sqlHandler) GetNotify(ctx context.Context, event string) (lis mercury.ListNotify, err error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	rows, err := squirrel.Select(`"name"`, `"match"`, `"event"`, `"method"`, `"url"`, `"rule"`).
		From("mercury_notify_vw").
		Where(squirrel.Eq{"event": event}).
		PlaceholderFormat(squirrel.Dollar).
		RunWith(pgm.db).
		QueryContext(context.TODO())

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		var s mercury.Notify
		var rule string
		err = rows.Scan(&s.Name, &s.Match, &s.Event, &s.Method, &s.URL, &rule)
		if err != nil {
			return nil, err
		}
		if rule != "" {
			s.Match, rule, _ = strings.Cut(rule, " ")
			s.Event, rule, _ = strings.Cut(rule, " ")
			s.Method, s.URL, _ = strings.Cut(rule, " ")
		}
		lis = append(lis, s)
	}

	return lis, rows.Err()
}
