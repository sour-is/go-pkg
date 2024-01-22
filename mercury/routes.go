package mercury

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/golang/gddo/httputil"
	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/ident"
)

type root struct{}

func NewHTTP() *root {
	return &root{}
}

func (s *root) RegisterHTTP(mux *http.ServeMux) {
	mux.Handle("/", http.FileServer(http.Dir("./mercury/public")))
}
func (s *root) RegisterAPIv1(mux *http.ServeMux) {
	mux.HandleFunc("GET /mercury", s.indexV1)
	// mux.HandleFunc("/mercury/config", s.configV1)
	mux.HandleFunc("GET /mercury/config", s.configV1)
	mux.HandleFunc("POST /mercury/config", s.storeV1)
}

func (s *root) configV1(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		s.storeV1(w, r)
		return
	}

	ctx, span := lg.Span(r.Context())
	defer span.End()

	var id ident.Ident = ident.FromContext(ctx)

	if !id.Session().Active {
		span.RecordError(fmt.Errorf("NO_AUTH"))
		http.Error(w, "NO_AUTH", http.StatusUnauthorized)
		return
	}

	rules, err := Registry.GetRules(ctx, id)
	if err != nil {
		span.RecordError(err)
		http.Error(w, "ERR: "+err.Error(), http.StatusInternalServerError)
		return
	}
	space := r.URL.Query().Get("space")
	if space == "" {
		space = "*"
	}

	log.Print("SPC:  ", space)
	ns := ParseNamespace(space)
	log.Print("PRE:  ", ns)
	ns = rules.ReduceSearch(ns)
	log.Print("POST: ", ns)

	lis, err := Registry.GetConfig(ctx, ns.String(), "", "")
	if err != nil {
		http.Error(w, "ERR: "+err.Error(), http.StatusInternalServerError)
		return
	}

	lis, err = Registry.accessFilter(rules, lis)
	if err != nil {
		span.RecordError(err)
		http.Error(w, "ERR: "+err.Error(), http.StatusInternalServerError)
		return
	}

	sort.Sort(lis)
	var content string

	switch httputil.NegotiateContentType(r, []string{
		"text/plain",
		"text/html",
		"application/environ",
		"application/ini",
		"application/json",
		"application/toml",
	}, "text/plain") {
	case "text/plain":
		content = lis.String()
	case "text/html":
		content = lis.HTMLString()
	case "application/environ":
		content = lis.EnvString()
	case "application/ini":
		content = lis.INIString()
	case "application/json":
		json.NewEncoder(w).Encode(lis)
	case "application/toml":
		w.WriteHeader(200)
		m := make(map[string]map[string][]string)
		for _, o := range lis {
			if _, ok := m[o.Space]; !ok {
				m[o.Space] = make(map[string][]string)
			}
			for _, v := range o.List {
				m[o.Space][v.Name] = append(m[o.Space][v.Name], v.Values...)
			}
		}
		err := toml.NewEncoder(w).Encode(m)
		if err != nil {
			// log.Error(err)
			http.Error(w, "ERR", http.StatusInternalServerError)
			return
		}
	}

	fmt.Fprint(w, content)
}

func (s *root) storeV1(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	var id = ident.FromContext(ctx)

	if !id.Session().Active {
		span.RecordError(fmt.Errorf("NO_AUTH"))
		http.Error(w, "NO_AUTH", http.StatusUnauthorized)
		return
	}

	var config SpaceMap
	var err error
	contentType, _, _ := strings.Cut(r.Header.Get("Content-Type"), ";")
	switch contentType {
	case "text/plain":
		config, err = ParseText(r.Body)
		r.Body.Close()
	case "application/x-www-form-urlencoded":
		r.ParseForm()
		config, err = ParseText(strings.NewReader(r.Form.Get("content")))
	case "multipart/form-data":
		r.ParseMultipartForm(1 << 20)
		config, err = ParseText(strings.NewReader(r.Form.Get("content")))
	default:
		http.Error(w, "PARSE_ERR", http.StatusUnsupportedMediaType)
		return
	}
	if err != nil {
		span.RecordError(err)
		http.Error(w, "PARSE_ERR", 400)
		return
	}

	{
		rules, err := Registry.GetRules(ctx, id)
		if err != nil {
			span.RecordError(err)
			http.Error(w, "ERR: "+err.Error(), http.StatusInternalServerError)
			return
		}

		notify, err := Registry.GetNotify(ctx, "updated")
		if err != nil {
			span.RecordError(err)
			http.Error(w, "ERR", http.StatusInternalServerError)
			return
		}
		_ = rules
		var notifyActive = make(map[string]struct{})
		var filteredConfigs Config
		for ns, c := range config {
			if !rules.GetRoles("NS", ns).HasRole("write") {
				span.AddEvent(fmt.Sprint("SKIP", ns))
				continue
			}

			span.AddEvent(fmt.Sprint("SAVE", ns))
			for _, n := range notify.Find(ns) {
				notifyActive[n.Name] = struct{}{}
			}
			filteredConfigs = append(filteredConfigs, c)
		}

		err = Registry.WriteConfig(ctx, filteredConfigs)
		if err != nil {
			span.RecordError(err)
			http.Error(w, "ERR", http.StatusInternalServerError)
			return
		}

		span.AddEvent(fmt.Sprint("SEND NOTIFYS ", notifyActive))
		for _, n := range notify {
			if _, ok := notifyActive[n.Name]; ok {
				err = Registry.SendNotify(ctx, n)
				if err != nil {
					span.RecordError(err)
					http.Error(w, "ERR", http.StatusInternalServerError)
					return
				}
			}
		}
		span.AddEvent("DONE!")
	}

	w.WriteHeader(202)
	fmt.Fprint(w, "OK")
}

func (s *root) indexV1(w http.ResponseWriter, r *http.Request) {
	ctx, span := lg.Span(r.Context())
	defer span.End()

	var id = ident.FromContext(ctx)

	timer := time.Now()
	defer func() { fmt.Println(time.Since(timer)) }()

	if !id.Session().Active {
		span.RecordError(fmt.Errorf("NO_AUTH"))
		http.Error(w, "NO_AUTH", http.StatusUnauthorized)
		return
	}

	rules, err := Registry.GetRules(ctx, id)
	if err != nil {
		span.RecordError(err)
		http.Error(w, "ERR: "+err.Error(), http.StatusInternalServerError)
		return
	}

	span.AddEvent(fmt.Sprint(rules))

	space := r.URL.Query().Get("space")
	if space == "" {
		space = "*"
	}

	ns := ParseNamespace(space)
	ns = rules.ReduceSearch(ns)
	span.AddEvent(ns.String())

	lis, err := Registry.GetIndex(ctx, ns.String(), "")
	if err != nil {
		span.RecordError(err)
		http.Error(w, "ERR: "+err.Error(), http.StatusInternalServerError)
		return
	}

	sort.Sort(lis)

	switch httputil.NegotiateContentType(r, []string{
		"text/plain",
		"application/json",
	}, "text/plain") {
	case "text/plain":
		_, err = fmt.Fprint(w, lis.StringList())
		span.RecordError(err)
	case "application/json":
		err = json.NewEncoder(w).Encode(lis)
		span.RecordError(err)
	}
}
