// SPDX-FileCopyrightText: 2023 Jon Lundy <jon@xuu.cc>
// SPDX-License-Identifier: BSD-3-Clause

package env

import (
	"log/slog"
	"os"
	"strings"
)

func Default(name, defaultValue string) string {
	name = strings.TrimSpace(name)
	defaultValue = strings.TrimSpace(defaultValue)
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		slog.Info("env", name, v)
		return v
	}
	slog.Info("env", name, defaultValue+" (default)")
	return defaultValue
}

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
func Secret(name, defaultValue string) secret {
	name = strings.TrimSpace(name)
	defaultValue = strings.TrimSpace(defaultValue)
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		slog.Info("env", name, secret(v))
		return secret(v)
	}
	slog.Info("env", name, secret(defaultValue).String()+" (default)")
	return secret(defaultValue)
}
