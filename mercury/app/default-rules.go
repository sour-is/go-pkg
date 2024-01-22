package app

import (
	"context"
	"strings"

	"go.sour.is/pkg/mercury"
	"go.sour.is/pkg/ident"
)

type mercuryDefault struct {
	name string
	cfg mercury.SpaceMap
}

var (
	_ mercury.GetRules = (*mercuryDefault)(nil)

	_ mercury.GetIndex  = (*mercuryEnviron)(nil)
	_ mercury.GetConfig = (*mercuryEnviron)(nil)
	_ mercury.GetRules  = (*mercuryEnviron)(nil)
)

// GetRules returns default rules for user role.
func (app *mercuryDefault) GetRules(ctx context.Context, id ident.Ident) (lis mercury.Rules, err error) {
	identity := id.Identity()

	lis = append(lis,
		mercury.Rule{
			Role:  "write",
			Type:  "NS",
			Match: "mercury.@" + identity,
		},
		mercury.Rule{
			Role:  "write",
			Type:  "NS",
			Match: "mercury.@" + identity + ".*",
		},
	)

	groups := groups(identity, &app.cfg)

	if s, ok := app.cfg.Space("mercury.policy."+app.name); ok {
		for _, p := range s.List {
			if groups.Has(p.Name) {
				for _, r := range p.Values {
					fds := strings.Fields(r)
					if len(fds) < 3 {
						continue
					}
					lis = append(lis, mercury.Rule{
						Role:  fds[0],
						Type:  fds[1],
						Match: fds[2],
					})
				}
			}
		}
	}

	if u, ok := id.(hasRole); groups.Has("admin") || ok && u.HasRole("admin") {
		lis = append(lis,
			mercury.Rule{
				Role:  "admin",
				Type:  "NS",
				Match: "*",
			},
			mercury.Rule{
				Role:  "write",
				Type:  "NS",
				Match: "*",
			},
			mercury.Rule{
				Role:  "admin",
				Type:  "GR",
				Match: "*",
			},
		)
	} else if u.HasRole("write") {
		lis = append(lis,
			mercury.Rule{
				Role:  "write",
				Type:  "NS",
				Match: "*",
			},
		)
	} else if u.HasRole("read") {
		lis = append(lis,
			mercury.Rule{
				Role:  "read",
				Type:  "NS",
				Match: "*",
			},
		)
	}

	return lis, nil
}
