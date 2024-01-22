package ident

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/oklog/ulid/v2"

	"go.sour.is/passwd"
	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/locker"
)

var (
	loginForm = func(nick string, valid bool) string {
		indicator := ""
		if !valid {
			indicator = `class="invalid"`
		}
		if nick != "" {
			nick = `value="` + nick + `"`
		}
		return `
	<form id="login" hx-post="ident/login" hx-target="#login" hx-swap="outerHTML">
		<input required id="login-identity" name="identity" type="text" ` + nick + `placeholder="Identity..." />
		<input required id="login-passwd" name="passwd" type="password" ` + indicator + ` placeholder="Password..." />

		<button type="submit">Login</button>
		<button hx-get="ident/register">Register</button>
	</form>`
	}
	logoutForm = func(display string) string {
		return `<button id="login" hx-post="ident/logout" hx-target="#login" hx-swap="outerHTML">` + display + ` (logout)</button>`
	}
	registerForm = `
	<form id="login" hx-post="ident/register" hx-target="#login" hx-swap="outerHTML">
		<input required id="register-display" name="displayName" type="text" placeholder="Display Name..." />
		<input required id="register-identity" name="identity" type="text" placeholder="Identity..." />
		<input required id="register-passwd" name="passwd" type="password" placeholder="Password..." />

		<button type="submit">Register</button>
		<button hx-get="ident" hx-target="#login" hx-swap="outerHTML">Close</button>
	</form>`
)

type sessions map[ulid.ULID]Ident

type root struct {
	idm      *IDM
	sessions *locker.Locked[sessions]
}

func NewHTTP(idm *IDM) *root {
	sessions := make(sessions)
	return &root{
		idm:      idm,
		sessions: locker.New(sessions),
	}
}

func (s *root) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/ident", s.get)
	mux.HandleFunc("/ident/register", s.register)
	mux.HandleFunc("/ident/login", s.login)
	mux.HandleFunc("/ident/logout", s.logout)
}
func (s *root) RegisterAPIv1(mux *http.ServeMux) {
	mux.HandleFunc("POST /ident", s.registerV1)
	mux.HandleFunc("POST /ident/session", s.loginV1)
	mux.HandleFunc("DELETE /ident/session", s.logoutV1)
	mux.HandleFunc("GET /ident", s.getV1)
}
func (s *root) RegisterMiddleware(hdlr http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := lg.Span(r.Context())
		defer span.End()

		cookie, err := r.Cookie("sour.is-ident")
		span.RecordError(err)
		if err != nil {
			hdlr.ServeHTTP(w, r)
			return
		}

		sessionID, err := ulid.Parse(cookie.Value)
		span.RecordError(err)

		var id Ident = Anonymous
		if err == nil {
			err = s.sessions.Use(ctx, func(ctx context.Context, sessions sessions) error {
				if session, ok := sessions[sessionID]; ok {
					id = session
				}
				return nil
			})
		}
		span.RecordError(err)

		r = r.WithContext(context.WithValue(r.Context(), contextKey, id))

		hdlr.ServeHTTP(w, r)
	})
}
func (s *root) createSession(ctx context.Context, id Ident) error {
	return s.sessions.Use(ctx, func(ctx context.Context, sessions sessions) error {
		sessions[id.Session().SessionID] = id
		return nil
	})
}
func (s *root) destroySession(ctx context.Context, id Ident) error {
	session := id.Session()
	session.Active = false

	return s.sessions.Use(ctx, func(ctx context.Context, sessions sessions) error {
		delete(sessions, session.SessionID)
		return nil
	})
}

func (s *root) getV1(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	var id Ident = FromContext(ctx)
	if id == nil {
		http.Error(w, "NO_AUTH", http.StatusUnauthorized)
		return
	}
	fmt.Fprint(w, id)
}
func (s *root) loginV1(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	id, err := s.idm.ReadIdent(r)
	span.RecordError(err)
	if err != nil {
		http.Error(w, "ERR", http.StatusInternalServerError)
		return
	}
	if !id.Session().Active {
		http.Error(w, "NO_AUTH", http.StatusUnauthorized)
		return
	}

	err = s.createSession(ctx, id)
	if err != nil {
		span.RecordError(err)
		http.Error(w, "ERR", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "sour.is-ident",
		Value:    id.Session().SessionID.String(),
		Expires:  time.Time{},
		Path:     "/",
		Secure:   false,
		HttpOnly: true,
	})

	fmt.Fprint(w, id)
}
func (s *root) logoutV1(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	if r.Method != http.MethodPost {
		http.Error(w, "ERR", http.StatusMethodNotAllowed)
		return
	}

	err := s.destroySession(ctx, FromContext(ctx))
	if err != nil {
		span.RecordError(err)
		http.Error(w, "NO_AUTH", http.StatusUnauthorized)
		return
	}

	http.SetCookie(w, &http.Cookie{Name: "sour.is-ident", MaxAge: -1})

	http.Error(w, "GONE", http.StatusGone)
}
func (s *root) registerV1(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	if r.Method != http.MethodPost {
		http.Error(w, "ERR", http.StatusMethodNotAllowed)
		return
	}
	r.ParseForm()

	identity := r.Form.Get("identity")
	display := r.Form.Get("displayName")
	passwd, err := s.idm.Passwd([]byte(r.Form.Get("passwd")), nil)
	if err != nil {
		http.Error(w, "ERR", http.StatusInternalServerError)
		return
	}

	_, err = s.idm.RegisterIdent(ctx, identity, display, passwd)
	if err != nil {
		http.Error(w, "ERR", http.StatusInternalServerError)
		return
	}

	http.Error(w, "OK "+identity, http.StatusCreated)
}

func (s *root) get(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	var id Ident = FromContext(ctx)
	if id == nil {
		http.Error(w, loginForm("", true), http.StatusOK)
		return
	}

	if !id.Session().Active {
		http.Error(w, loginForm("", true), http.StatusOK)
		return
	}

	display := id.Identity()
	if id, ok := id.(interface{ DisplayName() string }); ok {
		display = id.DisplayName()
	}
	fmt.Fprint(w, logoutForm(display))
}
func (s *root) login(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	if r.Method == http.MethodGet {
		fmt.Fprint(w, loginForm("", true))
		return
	}

	id, err := s.idm.ReadIdent(r)
	span.RecordError(err)
	if err != nil {
		if errors.Is(err, passwd.ErrNoMatch) {
			http.Error(w, loginForm("", false), http.StatusOK)
			return
		}

		http.Error(w, "ERROR", http.StatusInternalServerError)
		return
	}

	if !id.Session().Active {
		http.Error(w, loginForm("", false), http.StatusOK)
		return
	}

	err = s.createSession(ctx, id)
	span.RecordError(err)
	if err != nil {
		http.Error(w, "ERROR", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "sour.is-ident",
		Value:    id.Session().SessionID.String(),
		Expires:  time.Time{},
		Path:     "/",
		Secure:   false,
		HttpOnly: true,
	})

	display := id.Identity()
	if id, ok := id.(interface{ DisplayName() string }); ok {
		display = id.DisplayName()
	}
	fmt.Fprint(w, logoutForm(display))
}
func (s *root) logout(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	if r.Method != http.MethodPost {
		http.Error(w, "ERR", http.StatusMethodNotAllowed)
		return
	}

	http.SetCookie(w, &http.Cookie{Name: "sour.is-ident", MaxAge: -1})

	err := s.destroySession(ctx, FromContext(ctx))
	span.RecordError(err)
	if err != nil {
		http.Error(w, loginForm("", true), http.StatusUnauthorized)
		return
	}

	fmt.Fprint(w, loginForm("", true))
}
func (s *root) register(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	if r.Method == http.MethodGet {
		fmt.Fprint(w, registerForm)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "ERR", http.StatusMethodNotAllowed)
		return
	}

	r.ParseForm()
	identity := r.Form.Get("identity")
	display := r.Form.Get("displayName")

	passwd, err := s.idm.Passwd([]byte(r.Form.Get("passwd")), nil)
	if err != nil {
		http.Error(w, "ERR", http.StatusInternalServerError)
		return
	}

	id, err := s.idm.RegisterIdent(ctx, identity, display, passwd)
	if err != nil {
		http.Error(w, "ERR", http.StatusInternalServerError)
		return
	}

	if !id.Session().Active {
		http.Error(w, loginForm("", false), http.StatusUnauthorized)
		return
	}

	err = s.createSession(ctx, id)
	span.RecordError(err)
	if err != nil {
		http.Error(w, "ERROR", http.StatusInternalServerError)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Name:     "sour.is-ident",
		Value:    id.Session().SessionID.String(),
		Expires:  time.Time{},
		Path:     "/",
		Secure:   false,
		HttpOnly: true,
	})

	display = id.Identity()
	if id, ok := id.(interface{ DisplayName() string }); ok {
		display = id.DisplayName()
	}

	http.Error(w, logoutForm(display), http.StatusCreated)
}
