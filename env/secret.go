package env

import (
	"os"
	"log/slog"
	"strings"
)

type secret string

func (s secret) String() string {
	if s == "" {
		return "(nil)"
	}
	return "***"
}

func (s secret) Secret() string {
	return string(s)
}

func Secret(name, defaultValue string) (s secret) {
	name = strings.TrimSpace(name)
	s = secret(strings.TrimSpace(defaultValue))

	if v, ok := os.LookupEnv(name); ok {
		s = secret(strings.TrimSpace(v))
		slog.Info("env", slog.String(name, s.String()))
		return
	}

	slog.Info("env", slog.String(name, s.String()+" (default)"))
	return
}

