package mercury

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"go.sour.is/pkg/ident"
	"go.sour.is/pkg/lg"
	"go.sour.is/pkg/set"
	"golang.org/x/sync/errgroup"
)

type GetIndex interface {
	GetIndex(context.Context, Search) (Config, error)
}
type GetConfig interface {
	GetConfig(context.Context, Search) (Config, error)
}
type WriteConfig interface {
	WriteConfig(context.Context, Config) error
}
type GetRules interface {
	GetRules(context.Context, ident.Ident) (Rules, error)
}
type GetNotify interface {
	GetNotify(context.Context, string) (ListNotify, error)
}
type SendNotify interface {
	SendNotify(context.Context, Notify) error
}

// type nobody struct{}

// func (nobody) IsActive() bool           { return true }
// func (nobody) Identity() string         { return "xuu" }
// func (nobody) HasRole(r ...string) bool { return true }

func (reg *registry) accessFilter(rules Rules, lis Config) (out Config, err error) {
	accessList := make(map[string]struct{})
	for _, o := range lis {
		if _, ok := accessList[o.Space]; ok {
			out = append(out, o)
			continue
		}

		if role := rules.GetRoles("NS", o.Space); role.HasRole("read", "write") && !role.HasRole("deny") {
			accessList[o.Space] = struct{}{}
			out = append(out, o)
		}
	}

	return
}

// HandlerItem a single handler matching
type matcher[T any] struct {
	Name     string
	Match    Search
	Priority int
	Handler  T
}
type matchers struct {
	getIndex    []matcher[GetIndex]
	getConfig   []matcher[GetConfig]
	writeConfig []matcher[WriteConfig]
	getRules    []matcher[GetRules]
	getNotify   []matcher[GetNotify]
	sendNotify  []matcher[SendNotify]
}

// registry a list of handlers
type registry struct {
	handlers map[string]func(*Space) any
	matchers matchers
}

func (m matcher[T]) String() string {
	return fmt.Sprintf("%d: %s", m.Priority, m.Match)
}

// Registry handler
var Registry *registry = &registry{}

func (r registry) String() string {
	var buf strings.Builder
	for h := range r.handlers {
		buf.WriteString(h)
		buf.WriteRune('\n')
	}

	return buf.String()
}

func (r *registry) resetMatchers() {
	r.matchers.getIndex = r.matchers.getIndex[:0]
	r.matchers.getConfig = r.matchers.getConfig[:0]
	r.matchers.writeConfig = r.matchers.writeConfig[:0]
	r.matchers.getRules = r.matchers.getRules[:0]
	r.matchers.getNotify = r.matchers.getNotify[:0]
	r.matchers.sendNotify = r.matchers.sendNotify[:0]
}
func (r *registry) sortMatchers() {
	sort.Slice(r.matchers.getConfig, func(i, j int) bool { return r.matchers.getConfig[i].Priority < r.matchers.getConfig[j].Priority })
	sort.Slice(r.matchers.getIndex, func(i, j int) bool { return r.matchers.getIndex[i].Priority < r.matchers.getIndex[j].Priority })
	sort.Slice(r.matchers.writeConfig, func(i, j int) bool { return r.matchers.writeConfig[i].Priority < r.matchers.writeConfig[j].Priority })
	sort.Slice(r.matchers.getRules, func(i, j int) bool { return r.matchers.getRules[i].Priority < r.matchers.getRules[j].Priority })
	sort.Slice(r.matchers.getNotify, func(i, j int) bool { return r.matchers.getNotify[i].Priority < r.matchers.getNotify[j].Priority })
	sort.Slice(r.matchers.sendNotify, func(i, j int) bool { return r.matchers.sendNotify[i].Priority < r.matchers.sendNotify[j].Priority })
}
func (r *registry) Register(name string, h func(*Space) any) {
	if r.handlers == nil {
		r.handlers = make(map[string]func(*Space) any)
	}
	r.handlers[name] = h
}

func (r *registry) Configure(m SpaceMap) error {
	r.resetMatchers()
	for space, c := range m {
		log.Println("configure: ", space)

		if strings.HasPrefix(space, "mercury.source.") {
			space = strings.TrimPrefix(space, "mercury.source.")
			handler, name, _ := strings.Cut(space, ".")
			matches := c.FirstValue("match")
			readonly := c.HasTag("readonly")
			for _, match := range matches.Values {
				ps := strings.Fields(match)
				priority, err := strconv.Atoi(ps[0])
				if err != nil {
					return err
				}
				err = r.add(name, handler, strings.Join(ps[1:],"|"), priority, c, readonly)
				if err != nil {
					return err
				}
			}
		}

		if strings.HasPrefix(space, "mercury.output.") {
			space = strings.TrimPrefix(space, "mercury.output.")
			handler, name, _ := strings.Cut(space, ".")
			matches := c.FirstValue("match")
			for _, match := range matches.Values {
				ps := strings.Fields(match)
				priority, err := strconv.Atoi(ps[0])
				if err != nil {
					return err
				}
				err = r.add(name, handler, strings.Join(ps[1:],"|"), priority, c, false)
				if err != nil {
					return err
				}
			}
		}
	}

	r.sortMatchers()
	return nil
}

// Register add a handler to registry
func (r *registry) add(name, handler, match string, priority int, cfg *Space, readonly bool) error {
	log.Println("mercury regster", "match", match, "pri", priority)
	mkHandler, ok := r.handlers[handler]
	if !ok {
		return fmt.Errorf("handler not registered: %s", handler)
	}
	hdlr := mkHandler(cfg)
	if err, ok := hdlr.(error); ok {
		return fmt.Errorf("%w: failed to config %s as handler: %s", err, name, handler)
	}
	if hdlr == nil {
		return fmt.Errorf("failed to config %s as handler: %s", name, handler)
	}

	if hdlr, ok := hdlr.(GetIndex); ok {
		r.matchers.getIndex = append(
			r.matchers.getIndex,
			matcher[GetIndex]{Name: name, Match: ParseSearch(match), Priority: priority, Handler: hdlr},
		)
	}
	if hdlr, ok := hdlr.(GetConfig); ok {
		r.matchers.getConfig = append(
			r.matchers.getConfig,
			matcher[GetConfig]{Name: name, Match: ParseSearch(match), Priority: priority, Handler: hdlr},
		)
	}

	if hdlr, ok := hdlr.(WriteConfig); !readonly && ok {

		r.matchers.writeConfig = append(
			r.matchers.writeConfig,
			matcher[WriteConfig]{Name: name, Match: ParseSearch(match), Priority: priority, Handler: hdlr},
		)
	}
	if hdlr, ok := hdlr.(GetRules); ok {
		r.matchers.getRules = append(
			r.matchers.getRules,
			matcher[GetRules]{Name: name, Match: ParseSearch(match), Priority: priority, Handler: hdlr},
		)
	}
	if hdlr, ok := hdlr.(GetNotify); ok {
		r.matchers.getNotify = append(
			r.matchers.getNotify,
			matcher[GetNotify]{Name: name, Match: ParseSearch(match), Priority: priority, Handler: hdlr},
		)
	}
	if hdlr, ok := hdlr.(SendNotify); ok {
		r.matchers.sendNotify = append(
			r.matchers.sendNotify,
			matcher[SendNotify]{Name: name, Match: ParseSearch(match), Priority: priority, Handler: hdlr},
		)
	}

	return nil
}

func getMatches(search Search, matchers matchers) []Search {
	matches := make([]Search, len(matchers.getIndex))

	for _, n := range search.NamespaceSearch {
		for i, hdlr := range matchers.getIndex {
			if hdlr.Match.Match(n.Raw()) {
				matches[i].NamespaceSearch = append(matches[i].NamespaceSearch, n)
				matches[i].Count = search.Count
				matches[i].Cursor = search.Cursor // need to decode cursor for the match
				matches[i].Fields = search.Fields
				matches[i].Find = search.Find
			}
		}
	}
	return matches
}

// GetIndex query each handler that match namespace.
func (r *registry) GetIndex(ctx context.Context, search Search) (c Config, err error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	matches := getMatches(search, r.matchers)

	wg, ctx := errgroup.WithContext(ctx)
	slots := make(chan Config, len(r.matchers.getConfig))
	wg.Go(func() error {
		i := 0
		for lis := range slots {
			c = append(c, lis...)
			i++
			if i > len(slots) {
				break
			}
		}
		return nil
	})

	for i, hdlr := range r.matchers.getIndex {
		i, hdlr := i, hdlr

		wg.Go(func() error {
			span.AddEvent(fmt.Sprintf("INDEX %s %s", hdlr.Name, hdlr.Match))
			lis, err := hdlr.Handler.GetIndex(ctx, matches[i])
			slots <- lis
			return err
		})
	}

	err = wg.Wait()
	if err != nil {
		return nil, err
	}

	return
}

// Search query each handler with a key=value search

// GetConfig query each handler that match for fully qualified namespaces.
func (r *registry) GetConfig(ctx context.Context, search Search) (Config, error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	matches := getMatches(search, r.matchers)

	m := make(SpaceMap)
	for i, hdlr := range r.matchers.getConfig {
		if len(matches[i].NamespaceSearch) == 0 {
			continue
		}
		span.AddEvent(fmt.Sprintf("QUERY %s %s", hdlr.Name, hdlr.Match))
		lis, err := hdlr.Handler.GetConfig(ctx, matches[i])
		if err != nil {
			return nil, err
		}
		m.Merge(lis...)
	}

	return m.ToArray(), nil
}

// WriteConfig write objects to backends
func (r *registry) WriteConfig(ctx context.Context, spaces Config) error {
	ctx, span := lg.Span(ctx)
	defer span.End()

	matches := make([]Config, len(r.matchers.writeConfig))

	for _, s := range spaces {
		for i, hdlr := range r.matchers.writeConfig {
			if hdlr.Match.Match(s.Space) {
				matches[i] = append(matches[i], s)
				break
			}
		}
	}

	for i, hdlr := range r.matchers.writeConfig {
		if len(matches[i]) == 0 {
			continue
		}
		span.AddEvent(fmt.Sprint("WRITE MATCH", hdlr.Name, hdlr.Match))
		err := hdlr.Handler.WriteConfig(ctx, matches[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// GetRules query each of the handlers for rules.
func (r *registry) GetRules(ctx context.Context, user ident.Ident) (Rules, error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	s := set.New[Rule]()
	for _, hdlr := range r.matchers.getRules {
		span.AddEvent(fmt.Sprint("RULES", hdlr.Name, hdlr.Match))
		lis, err := hdlr.Handler.GetRules(ctx, user)
		if err != nil {
			return nil, err
		}
		s.Add(lis...)
	}
	var rules Rules = s.Values()
	sort.Sort(rules)
	return rules, nil
}

// GetNotify query each of the handlers for rules.
func (r *registry) GetNotify(ctx context.Context, event string) (ListNotify, error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	s := set.New[Notify]()
	for _, hdlr := range r.matchers.getNotify {
		span.AddEvent(fmt.Sprint("GET NOTIFY", hdlr.Name, hdlr.Match))

		lis, err := hdlr.Handler.GetNotify(ctx, event)
		if err != nil {
			return nil, err
		}
		s.Add(lis...)
	}

	return s.Values(), nil
}

func (r *registry) SendNotify(ctx context.Context, n Notify) (err error) {
	ctx, span := lg.Span(ctx)
	defer span.End()

	for _, hdlr := range r.matchers.sendNotify {
		span.AddEvent(fmt.Sprint("SEND NOTIFY", hdlr.Name, hdlr.Match))

		err := hdlr.Handler.SendNotify(ctx, n)
		if err != nil {
			return err
		}
	}

	return
}

// Check if name matches notify
func (n Notify) Check(name string) bool {
	ok, err := filepath.Match(n.Match, name)
	if err != nil {
		return false
	}
	return ok
}

// Notify stores the attributes for a registry space
type Notify struct {
	Name   string
	Match  string
	Event  string
	Method string
	URL    string
}

// ListNotify array of notify
type ListNotify []Notify

// Find returns list of notify that match name.
func (ln ListNotify) Find(name string) (lis ListNotify) {
	lis = make(ListNotify, 0, len(ln))
	for _, o := range ln {
		if o.Check(name) {
			lis = append(lis, o)
		}
	}
	return
}
