package source

import (
	"context"
	"net/http"
	"time"

	"github.com/oklog/ulid/v2"
	"go.sour.is/pkg/ident"
	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/locker"
)

const CookieName = "sour.is-ident"

type sessions map[ulid.ULID]ident.Ident

type session struct {
	cookieName string
	sessions   *locker.Locked[sessions]
}

func NewSession(cookieName string) *session {
	return &session{
		cookieName: cookieName,
		sessions:   locker.New(make(sessions)),
	}
}

func (s *session) ReadIdent(r *http.Request) (ident.Ident, error) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	cookie, err := r.Cookie(s.cookieName)
	span.RecordError(err)
	if err != nil {
		return nil, nil
	}

	sessionID, err := ulid.Parse(cookie.Value)
	span.RecordError(err)

	var id ident.Ident = ident.Anonymous
	if err == nil {
		err = s.sessions.Use(ctx, func(ctx context.Context, sessions sessions) error {
			if session, ok := sessions[sessionID]; ok {
				id = session
			}
			return nil
		})
	}
	span.RecordError(err)

	return id, err
}

func (s *session) CreateSession(ctx context.Context, w http.ResponseWriter, id ident.Ident) error {
	http.SetCookie(w, &http.Cookie{
		Name:     s.cookieName,
		Value:    id.Session().SessionID.String(),
		Expires:  time.Time{},
		Path:     "/",
		Secure:   false,
		HttpOnly: true,
	})

	return s.sessions.Use(ctx, func(ctx context.Context, sessions sessions) error {
		sessions[id.Session().SessionID] = id
		return nil
	})
}

func (s *session) DestroySession(ctx context.Context, w http.ResponseWriter, id ident.Ident) error {
	session := id.Session()
	session.Active = false

	http.SetCookie(w, &http.Cookie{Name: s.cookieName, MaxAge: -1})

	return s.sessions.Use(ctx, func(ctx context.Context, sessions sessions) error {
		delete(sessions, session.SessionID)
		return nil
	})
}
