package ident

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"

	"github.com/oklog/ulid/v2"
	"go.sour.is/passwd"
)

// Ident interface for a logged in user
type Ident interface {
	Identity() string
	Session() *SessionInfo
}

type SessionInfo struct {
	SessionID ulid.ULID
	Active    bool
}

func (s *SessionInfo) Session() *SessionInfo { return s }

// Handler handler function to read ident from HTTP request
type Handler interface {
	ReadIdent(r *http.Request) (Ident, error)
}
type HandleGet interface {
	GetIdent(context.Context /* identity */, string) (Ident, error)
}
type HandleRegister interface {
	RegisterIdent(ctx context.Context, identity, displayName string, passwd []byte) (Ident, error)
}

type source struct {
	Handler
	priority int
}

var contextKey = struct{ key string }{"ident"}

func FromContext(ctx context.Context) Ident {
	if id, ok := ctx.Value(contextKey).(Ident); ok {
		return id
	}
	return Anonymous
}

type IDM struct {
	rand    io.Reader
	sources []source
	pwd *passwd.Passwd
}

func NewIDM(pwd *passwd.Passwd, rand io.Reader) *IDM {
	return &IDM{pwd: pwd, rand:rand}
}

func (idm *IDM) Add(p int, h Handler) {
	idm.sources = append(idm.sources, source{priority: p, Handler: h})
	sort.Slice(idm.sources, func(i, j int) bool { return idm.sources[i].priority < idm.sources[j].priority })
}

func (idm *IDM) Passwd(pass, hash []byte) ([]byte, error) {
	return idm.pwd.Passwd(pass, hash)
}

// ReadIdent read ident from a list of ident handlers
func (idm *IDM) ReadIdent(r *http.Request) (Ident, error) {
	for _, source := range idm.sources {
		u, err := source.ReadIdent(r)
		if err != nil {
			return Anonymous, err
		}

		if u.Session().Active {
			return u, err
		}
	}

	return Anonymous, nil
}

func (idm *IDM) RegisterIdent(ctx context.Context, identity, displayName string, passwd []byte) (Ident, error) {
	for _, source := range idm.sources {
		if source, ok := source.Handler.(HandleRegister); ok {
			return source.RegisterIdent(ctx, identity, displayName, passwd)
		}
	}

	return nil, fmt.Errorf("no HandleRegister source registered")
}

func (idm *IDM) GetIdent(ctx context.Context, identity string) (Ident, error) {
	for _, source := range idm.sources {
		if source, ok := source.Handler.(HandleGet); ok {
			return source.GetIdent(ctx, identity)
		}
	}

	return nil, fmt.Errorf("no HandleGet source registered")
}

func (idm *IDM) NewSessionInfo() (session SessionInfo, err error) {
	session.SessionID, err = ulid.New(ulid.Now(), idm.rand)
	if err != nil {
		return
	}
	session.Active = true

	return session, nil
}
