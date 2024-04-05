package source

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/mercury"
	"go.sour.is/pkg/ident"
)

const identNS = "ident."
const identSFX = ".credentials"

type registry interface {
	GetIndex(ctx context.Context, match, search string) (c mercury.Config, err error)
	GetConfig(ctx context.Context, match, search, fields string) (mercury.Config, error)
	WriteConfig(ctx context.Context, spaces mercury.Config) error
}

type mercuryIdent struct {
	identity string
	display  string
	passwd   []byte
	ed25519  []byte
	ident.SessionInfo
}

func (id *mercuryIdent) Identity() string    { return id.identity }
func (id *mercuryIdent) DisplayName() string { return id.display }
func (id *mercuryIdent) Space() string       { return identNS + "@" + id.identity }

func (id *mercuryIdent) FromConfig(cfg mercury.Config) error {
	if id == nil {
		return fmt.Errorf("nil ident")
	}

	for _, s := range cfg {
		if !strings.HasPrefix(s.Space, identNS) {
			continue
		}
		if id.identity == "" {
			_, id.identity, _ = strings.Cut(s.Space, ".@")
			id.identity, _, _ = strings.Cut(id.identity, ".")
		}

		switch {
		case strings.HasSuffix(s.Space, ".credentials"):
			id.passwd = []byte(s.FirstValue("passwd").First())
		default:
			id.display = s.FirstValue("displayName").First()
		}
	}
	return nil
}

func (id *mercuryIdent) ToConfig() mercury.Config {
	space := id.Space()
	return mercury.Config{
		&mercury.Space{
			Space: space,
			List: []mercury.Value{
				{
					Space:  space,
					Seq:    1,
					Name:   "displayName",
					Values: []string{id.display},
				},
				{
					Space:  space,
					Seq:    2,
					Name:   "lastLogin",
					Values: []string{time.UnixMilli(int64(id.Session().SessionID.Time())).Format(time.RFC3339)},
				},
			},
		},
		&mercury.Space{
			Space: space + identSFX,
			List: []mercury.Value{
				{
					Space:  space + identSFX,
					Seq:    1,
					Name:   "passwd",
					Values: []string{string(id.passwd)},
				},
			},
		},
	}
}

func (id *mercuryIdent) String() string {
	return "id: " + id.identity + " sp: " + id.Space() + " dn: " + id.display // + " ps: " + string(id.passwd)
}

func (id *mercuryIdent) HasRole(r ...string) bool {
	return false
}

type mercurySource struct {
	r   registry
	idm *ident.IDM
}

func NewMercury(r registry, pwd *ident.IDM) *mercurySource {
	return &mercurySource{r, pwd}
}

func (s *mercurySource) ReadIdent(r *http.Request) (ident.Ident, error) {
	if id, err := s.readIdentBasic(r); id != nil {
		return id, err
	}

	if id, err := s.readIdentURL(r); id != nil {
		return id, err
	}

	if id, err := s.readIdentHTTP(r); id != nil {
		return id, err
	}

	return nil, fmt.Errorf("no auth")
}

func (s *mercurySource) readIdentURL(r *http.Request) (ident.Ident, error) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	pass, ok := r.URL.User.Password()

	if !ok {
		return nil, nil
	}

	id := &mercuryIdent{
		identity: r.URL.User.Username(),
		passwd:   []byte(pass),
	}

	space := id.Space()
	c, err := s.r.GetConfig(ctx, "trace:"+space+identSFX, "", "")
	if err != nil {
		span.RecordError(err)
		return id, err
	}
	var current mercuryIdent
	current.FromConfig(c)
	if len(current.passwd) == 0 {
		return nil, fmt.Errorf("not registered")
	}
	_, err = s.idm.Passwd(id.passwd, current.passwd)
	if err != nil {
		return id, err
	}
	current.SessionInfo, err = s.idm.NewSessionInfo()
	if err != nil {
		return id, err
	}

	err = s.r.WriteConfig(ctx, current.ToConfig())
	if err != nil {
		return &current, err
	}

	return &current, nil
}

func (s *mercurySource) readIdentBasic(r *http.Request) (ident.Ident, error) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	user, pass, ok := r.BasicAuth()

	if !ok {
		return nil, nil
	}

	id := &mercuryIdent{
		identity: user,
		passwd:   []byte(pass),
	}

	space := id.Space()
	c, err := s.r.GetConfig(ctx, "trace:"+space+identSFX, "", "")
	if err != nil {
		span.RecordError(err)
		return id, err
	}
	var current mercuryIdent
	current.FromConfig(c)
	if len(current.passwd) == 0 {
		return nil, fmt.Errorf("not registered")
	}
	_, err = s.idm.Passwd(id.passwd, current.passwd)
	if err != nil {
		return id, err
	}
	current.SessionInfo, err = s.idm.NewSessionInfo()
	if err != nil {
		return id, err
	}

	err = s.r.WriteConfig(ctx, current.ToConfig())
	if err != nil {
		return &current, err
	}

	return &current, nil
}

func (s *mercurySource) readIdentHTTP(r *http.Request) (ident.Ident, error) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	if r.Method != http.MethodPost {
		return nil, fmt.Errorf("method not allowed")
	}
	r.ParseForm()
	id := &mercuryIdent{
		identity: r.Form.Get("identity"),
		passwd:   []byte(r.Form.Get("passwd")),
	}

	if id.identity == "" {
		return nil, nil
	}

	space := id.Space()
	c, err := s.r.GetConfig(ctx, "trace:"+space+identSFX, "", "")
	if err != nil {
		span.RecordError(err)
		return id, err
	}
	var current mercuryIdent
	current.FromConfig(c)
	if len(current.passwd) == 0 {
		return nil, fmt.Errorf("not registered")
	}
	_, err = s.idm.Passwd(id.passwd, current.passwd)
	if err != nil {
		return id, err
	}
	current.SessionInfo, err = s.idm.NewSessionInfo()
	if err != nil {
		return id, err
	}

	err = s.r.WriteConfig(ctx, current.ToConfig())
	if err != nil {
		return &current, err
	}

	return &current, nil
}

func (s *mercurySource) RegisterIdent(ctx context.Context, identity, display string, passwd []byte) (ident.Ident, error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	id := &mercuryIdent{identity: identity, display: display, passwd: passwd}
	space := id.Space()

	_, err := s.r.GetIndex(ctx, space, "")
	if err != nil {
		return nil, err
	}

	id.SessionInfo, err = s.idm.NewSessionInfo()
	if err != nil {
		return id, err
	}

	err = s.r.WriteConfig(ctx, id.ToConfig())
	if err != nil {
		return nil, err
	}
	return id, nil
}
