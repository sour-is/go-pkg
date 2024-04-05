package ident

import (
	"context"
	"fmt"
	"net/http"

	"go.sour.is/pkg/lg"
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
	<form id="login" hx-post="ident/session" hx-target="#login" hx-swap="outerHTML">
		<input required id="login-identity" name="identity" type="text" ` + nick + `placeholder="Identity..." />
		<input required id="login-passwd" name="passwd" type="password" ` + indicator + ` placeholder="Password..." />

		<button type="submit">Login</button>
		<button hx-get="ident/register">Register</button>
	</form>`
	}
	logoutForm = func(id Ident) string {
		display := id.Identity()
		if id, ok := id.(interface{ DisplayName() string }); ok {
			display = id.DisplayName()
		}
		return `<button id="login" hx-delete="ident/session" hx-target="#login" hx-swap="outerHTML">` + display + ` (logout)</button>`
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

type sessionIF interface {
	ReadIdent(r *http.Request) (Ident, error)
	CreateSession(context.Context, http.ResponseWriter, Ident) error
	DestroySession(context.Context, http.ResponseWriter, Ident) error
}

type root struct {
	idm     *IDM
	session sessionIF
}

func NewHTTP(idm *IDM, session sessionIF) *root {
	idm.Add(0, session)
	return &root{
		idm:     idm,
		session: session,
	}
}

func (s *root) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/ident", s.sessionHTTP)
	mux.HandleFunc("/ident/register", s.registerHTTP)
	mux.HandleFunc("/ident/session", s.sessionHTTP)
}
func (s *root) RegisterAPIv1(mux *http.ServeMux) {
	mux.HandleFunc("GET /ident", s.sessionV1)
	mux.HandleFunc("POST /ident", s.registerV1)
	mux.HandleFunc("/ident/session", s.sessionV1)
}
func (s *root) RegisterMiddleware(hdlr http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := lg.Span(r.Context())
		defer span.End()
		r = r.WithContext(ctx)

		id, err := s.idm.ReadIdent(r)
		span.RecordError(err)
		if id == nil {
			id = Anonymous
		}

		r = r.WithContext(context.WithValue(r.Context(), contextKey, id))

		hdlr.ServeHTTP(w, r)
	})
}

func (s *root) sessionV1(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	var id Ident = FromContext(ctx)
	switch r.Method {
	case http.MethodGet:
		if id == nil {
			http.Error(w, "NO_AUTH", http.StatusUnauthorized)
			return
		}
		fmt.Fprint(w, id)
	case http.MethodPost:
		if !id.Session().Active {
			http.Error(w, "NO_AUTH", http.StatusUnauthorized)
			return
		}
	
		err := s.session.CreateSession(ctx, w, id)
		if err != nil {
			span.RecordError(err)
			http.Error(w, "ERR", http.StatusInternalServerError)
			return
		}
	
		fmt.Fprint(w, id)
	
	case http.MethodDelete:
		if !id.Session().Active {
			http.Error(w, "NO_AUTH", http.StatusUnauthorized)
			return
		}

		err := s.session.DestroySession(ctx, w, FromContext(ctx))
		if err != nil {
			span.RecordError(err)
			http.Error(w, "ERR", http.StatusInternalServerError)
			return
		}
	
		http.Error(w, "GONE", http.StatusGone)
	
	default:
		http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		return
	}
}
func (s *root) registerV1(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

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

func (s *root) sessionHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	id := FromContext(ctx)

	switch r.Method {
	case http.MethodGet:
		if id.Session().Active {
			fmt.Fprint(w, logoutForm(id))
			return
		}
		fmt.Fprint(w, loginForm("", true))
	case http.MethodPost:
		if !id.Session().Active {
			http.Error(w, loginForm("", false), http.StatusOK)
			return
		}
	
		err := s.session.CreateSession(ctx, w, id)
		span.RecordError(err)
		if err != nil {
			http.Error(w, "ERROR", http.StatusInternalServerError)
			return
		}
	
		fmt.Fprint(w, logoutForm(id))
	case http.MethodDelete:
		err := s.session.DestroySession(ctx, w, FromContext(ctx))
		span.RecordError(err)
		if err != nil {
			http.Error(w, loginForm("", true), http.StatusUnauthorized)
			return
		}
	
		fmt.Fprint(w, loginForm("", true))
	default:
		http.Error(w, "ERROR", http.StatusMethodNotAllowed)
	}
}
func (s *root) registerHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	switch r.Method {
	case http.MethodGet:
		fmt.Fprint(w, registerForm)
		return
	case http.MethodPost:
		// break
	default:
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

	err = s.session.CreateSession(ctx, w, id)
	span.RecordError(err)
	if err != nil {
		http.Error(w, "ERROR", http.StatusInternalServerError)
		return
	}

	http.Error(w, logoutForm(id), http.StatusCreated)
}
