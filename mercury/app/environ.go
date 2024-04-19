package app

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"sort"
	"strings"

	"go.sour.is/pkg/ident"
	"go.sour.is/pkg/mercury"
	"go.sour.is/pkg/set"
)

const (
	mercurySource   = "mercury.source.*"
	mercuryPriority = "mercury.priority"
	mercuryHost     = "mercury.host"
	appDotEnviron   = "mercury.environ"
)

var (
	mercuryPolicy = func(id string) string { return "mercury.@" + id + ".policy" }
)

func Register(name string, cfg mercury.SpaceMap) {
	for _, c := range cfg {
		c.Tags = append(c.Tags, "RO")
	}
	mercury.Registry.Register("mercury-default", func(s *mercury.Space) any { return &mercuryDefault{name: name, cfg: cfg} })
	mercury.Registry.Register("mercury-environ", func(s *mercury.Space) any { return &mercuryEnviron{cfg: cfg, lookup: mercury.Registry.GetRules} })
}

type hasRole interface {
	HasRole(r ...string) bool
}

type mercuryEnviron struct {
	cfg    mercury.SpaceMap
	lookup func(context.Context, ident.Ident) (mercury.Rules, error)
}

func getSearch(spec mercury.Search) mercury.NamespaceSearch {
	return spec.NamespaceSearch
}

// Index returns nil
func (app *mercuryEnviron) GetIndex(ctx context.Context, spec mercury.Search) (lis mercury.Config, err error) {
	search := getSearch(spec)

	if search.Match(mercurySource) {
		for _, s := range app.cfg.ToArray() {
			if search.Match(s.Space) {
				lis = append(lis, &mercury.Space{Space: s.Space, Tags: []string{"RO"}})
			}
		}
	}

	if search.Match(mercuryPriority) {
		lis = append(lis, &mercury.Space{Space: mercuryPriority, Tags: []string{"RO"}})
	}

	if search.Match(mercuryHost) {
		lis = append(lis, &mercury.Space{Space: mercuryHost, Tags: []string{"RO"}})
	}

	if search.Match(appDotEnviron) {
		lis = append(lis, &mercury.Space{Space: appDotEnviron, Tags: []string{"RO"}})
	}
	if id := ident.FromContext(ctx); id != nil {
		identity := id.Identity()
		match := mercuryPolicy(identity)
		if search.Match(match) {
			lis = append(lis, &mercury.Space{Space: match, Tags: []string{"RO"}})
		}
	}
	return
}

// Objects returns nil
func (app *mercuryEnviron) GetConfig(ctx context.Context, spec mercury.Search) (lis mercury.Config, err error) {
	search := getSearch(spec)

	if search.Match(mercurySource) {
		for _, s := range app.cfg.ToArray() {
			if search.Match(s.Space) {
				lis = append(lis, s)
			}
		}
	}

	if search.Match(mercuryPriority) {
		space := mercury.Space{
			Space: mercuryPriority,
			Tags:  []string{"RO"},
		}

		// for i, key := range mercury.Registry {
		// 	space.List = append(space.List, mercury.Value{
		// 		Space:  appDotPriority,
		// 		Seq:    uint64(i),
		// 		Name:   key.Match,
		// 		Values: []string{fmt.Sprint(key.Priority)},
		// 	})
		// }

		lis = append(lis, &space)
	}

	if search.Match(mercuryHost) {
		if usr, err := user.Current(); err == nil {
			space := mercury.Space{
				Space: mercuryHost,
				Tags:  []string{"RO"},
			}

			hostname, _ := os.Hostname()
			wd, _ := os.Getwd()
			grp, _ := usr.GroupIds()
			space.List = []mercury.Value{
				{
					Space:  mercuryHost,
					Seq:    1,
					Name:   "hostname",
					Values: []string{hostname},
				},
				{
					Space:  mercuryHost,
					Seq:    2,
					Name:   "username",
					Values: []string{usr.Username},
				},
				{
					Space:  mercuryHost,
					Seq:    3,
					Name:   "uid",
					Values: []string{usr.Uid},
				},
				{
					Space:  mercuryHost,
					Seq:    4,
					Name:   "gid",
					Values: []string{usr.Gid},
				},
				{
					Space:  mercuryHost,
					Seq:    5,
					Name:   "display",
					Values: []string{usr.Name},
				},
				{
					Space:  mercuryHost,
					Seq:    6,
					Name:   "home",
					Values: []string{usr.HomeDir},
				},
				{
					Space:  mercuryHost,
					Seq:    7,
					Name:   "groups",
					Values: grp,
				},
				{
					Space:  mercuryHost,
					Seq:    8,
					Name:   "pid",
					Values: []string{fmt.Sprintf("%v", os.Getpid())},
				},
				{
					Space:  mercuryHost,
					Seq:    9,
					Name:   "wd",
					Values: []string{wd},
				},
			}

			lis = append(lis, &space)
		}
	}

	if search.Match(appDotEnviron) {
		env := os.Environ()
		space := mercury.Space{
			Space: appDotEnviron,
			Tags:  []string{"RO"},
		}

		sort.Strings(env)
		for i, s := range env {
			key, val, _ := strings.Cut(s, "=")

			vals := []string{val}
			if strings.Contains(key, "PATH") || strings.Contains(key, "XDG") {
				vals = strings.Split(val, ":")
			}

			space.List = append(space.List, mercury.Value{
				Space:  appDotEnviron,
				Seq:    uint64(i),
				Name:   key,
				Values: vals,
			})
		}
		lis = append(lis, &space)
	}

	if id := ident.FromContext(ctx); id != nil {
		identity := id.Identity()
		groups := groups(identity, &app.cfg)
		match := mercuryPolicy(identity)
		if search.Match(match) {
			space := &mercury.Space{
				Space: match,
				Tags:  []string{"RO"},
			}

			lis = append(lis, space)
			rules, err := app.lookup(ctx, id)
			if err != nil {
				space.AddNotes(err.Error())
			} else {
				k := mercury.NewValue("groups")
				k.AddValues(strings.Join(groups.Values(), " "))
				space.AddKeys(k)

				k = mercury.NewValue("rules")
				for _, r := range rules {
					k.AddValues(strings.Join([]string{r.Role, r.Type, r.Match}, " "))
				}
				space.AddKeys(k)
			}
		}
	}

	return
}

// Rules returns nil
func (app *mercuryEnviron) GetRules(ctx context.Context, id ident.Ident) (lis mercury.Rules, err error) {
	identity := id.Identity()

	lis = append(lis,
		mercury.Rule{
			Role:  "read",
			Type:  "NS",
			Match: mercuryPolicy(identity),
		},
	)

	groups := groups(identity, &app.cfg)

	if u, ok := id.(hasRole); groups.Has("admin") || ok && u.HasRole("admin") {
		lis = append(lis,
			mercury.Rule{
				Role:  "read",
				Type:  "NS",
				Match: mercurySource,
			},
			mercury.Rule{
				Role:  "read",
				Type:  "NS",
				Match: mercuryPriority,
			},
			mercury.Rule{
				Role:  "read",
				Type:  "NS",
				Match: mercuryHost,
			},
			mercury.Rule{
				Role:  "read",
				Type:  "NS",
				Match: appDotEnviron,
			},
		)
	}

	return lis, nil
}

func groups(identity string, cfg *mercury.SpaceMap) set.Set[string] {
	groups := set.New[string]()
	if s, ok := cfg.Space("mercury.groups"); ok {
		for _, g := range s.List {
			for _, v := range g.Values {
				for _, u := range strings.Fields(v) {
					if u == identity {
						groups.Add(g.Name)
					}
				}
			}
		}
	}
	return groups
}
