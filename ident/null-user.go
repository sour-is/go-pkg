package ident

import "net/http"

// nullUser implements a null ident
type nullUser struct {
	identity    string
	aspect      string
	displayName string
	SessionInfo
}

// Anonymous is a logged out user
var Anonymous = NewNullUser("anon", "none", "Guest User", false)

// NewNullUser creates a null user ident
func NewNullUser(ident, aspect, name string, active bool) *nullUser {
	return &nullUser{ident, aspect, name, SessionInfo{Active: active}}
}

func (id nullUser) String() string {
	return "id: " + id.identity + " dn: " + id.displayName
}

// GetIdentity returns identity
func (m nullUser) Identity() string {
	return m.identity
}

// GetAspect returns aspect
func (m nullUser) Aspect() string {
	return m.aspect
}

// HasRole returns true if matches role
func (m nullUser) Role(r ...string) bool {
	return m.Active
}

// HasGroup returns true if matches group
func (m nullUser) Group(g ...string) bool {
	return m.Active
}

// GetGroups returns empty list
func (m nullUser) Groups() []string {
	return []string{}
}

// GetRoles returns empty list
func (m nullUser) Roles() []string {
	return []string{}
}

// GetMeta returns empty list
func (m nullUser) Meta() map[string]string {
	return make(map[string]string)
}

// IsActive returns true if active
func (m nullUser) IsActive() bool {
	return m.Active
}

// GetDisplay returns display name
func (m nullUser) Display() string {
	return m.displayName
}

// MakeHandlerFunc returns handler func
func (m nullUser) HandlerFunc() func(r *http.Request) Ident {
	return func(r *http.Request) Ident {
		return &m
	}
}
