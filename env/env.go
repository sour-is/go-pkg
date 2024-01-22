// SPDX-FileCopyrightText: 2023 Jon Lundy <jon@xuu.cc>
// SPDX-License-Identifier: BSD-3-Clause

package env

import (
	"log/slog"
	"os"
	"strings"
)

func Default(name, defaultValue string) (s string) {
	name = strings.TrimSpace(name)
	s = strings.TrimSpace(defaultValue)

	if v, ok := os.LookupEnv(name); ok {
		s = strings.TrimSpace(v)
		slog.Info("env", slog.String(name, v))
		return
	}

	slog.Info("env", slog.String(name, s+" (default)"))
	return
}
