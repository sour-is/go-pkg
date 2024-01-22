package app

import (
	"context"
	"reflect"
	"testing"

	"go.sour.is/pkg/mercury"
	"go.sour.is/pkg/ident"
)

type mockUser struct {
	roles  map[string]struct{}
	ident.SessionInfo
}

func (m *mockUser) Identity() string { return "user" }
func (m *mockUser) HasRole(roles ...string) bool {
	var found bool
	for _, role := range roles {
		if _, ok := m.roles[role]; ok {
			found = true
		}
	}

	return found
}

func Test_appConfig_GetRules(t *testing.T) {
	type args struct {
		u ident.Ident
	}
	tests := []struct {
		name    string
		args    args
		wantLis mercury.Rules
	}{
		{"normal", args{&mockUser{}}, nil},
		{
			"admin",
			args{
				&mockUser{
					SessionInfo: ident.SessionInfo{Active: true},
					roles:  map[string]struct{}{"admin": {}},
				},
			},
			mercury.Rules{
				mercury.Rule{
					Role:  "read",
					Type:  "NS",
					Match: "mercury.source.*",
				},
				mercury.Rule{
					Role:  "read",
					Type:  "NS",
					Match: "mercury.priority",
				},
				mercury.Rule{
					Role:  "read",
					Type:  "NS",
					Match: "mercury.host",
				},
				mercury.Rule{
					Role:  "read",
					Type:  "NS",
					Match: "mercury.environ",
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := mercuryEnviron{}
			if gotLis, _ := a.GetRules(context.TODO(), tt.args.u); !reflect.DeepEqual(gotLis, tt.wantLis) {
				t.Errorf("appConfig.GetRules() = %v, want %v", gotLis, tt.wantLis)
			}
		})
	}
}

// func Test_appConfig_GetIndex(t *testing.T) {
// 	type args struct {
// 		search mercury.NamespaceSearch
// 		in1    *rsql.Program
// 	}
// 	tests := []struct {
// 		name    string
// 		args    args
// 		wantLis mercury.Config
// 	}{
// 		{"nil", args{
// 			nil,
// 			nil,
// 		}, nil},

// 		{"app.settings", args{
// 			mercury.ParseNamespace("app.settings"),
// 			nil,
// 		}, mercury.Config{&mercury.Space{Space: "app.settings"}}},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			a := mercuryEnviron{}
// 			if gotLis, _ := a.GetIndex(tt.args.search, tt.args.in1); !reflect.DeepEqual(gotLis, tt.wantLis) {
// 				t.Errorf("appConfig.GetIndex() = %#v, want %#v", gotLis, tt.wantLis)
// 			}
// 		})
// 	}
// }

// func Test_appConfig_GetObjects(t *testing.T) {
// 	cfg, err := mercury.ParseText(strings.NewReader(`
// @mercury.source.mercury-settings.default
// match :0 *
// 	`))

// 	type args struct {
// 		search mercury.NamespaceSearch
// 		in1    *rsql.Program
// 		in2    []string
// 	}
// 	tests := []struct {
// 		name    string
// 		args    args
// 		wantLis mercury.Config
// 	}{
// 		{"nil", args{
// 			nil,
// 			nil,
// 			nil,
// 		}, nil},

// 		{"app.settings", args{
// 			mercury.ParseNamespace("app.settings"),
// 			nil,
// 			nil,
// 		}, mercury.Config{
// 			&mercury.Space{
// 				Space: "app.settings",
// 				List: []mercury.Value{{
// 					Space: "app.settings",
// 					Name: "app.setting",
// 					Values: []string{"TRUE"}},
// 				},
// 			},
// 		}},
// 	}
// 	for _, tt := range tests {
// 		cfg, err :=
// 		t.Run(tt.name, func(t *testing.T) {
// 			a := appConfig{cfg: }
// 			if gotLis, _ := a.GetConfig(tt.args.search, tt.args.in1, tt.args.in2); !reflect.DeepEqual(gotLis, tt.wantLis) {
// 				t.Errorf("appConfig.GetIndex() = %#v, want %#v", gotLis, tt.wantLis)
// 			}
// 		})
// 	}
// }
