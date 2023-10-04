package xdg_test

import (
	"strings"
	"testing"

	"github.com/matryer/is"

	"go.sour.is/pkg/xdg"
)

func TestGet(t *testing.T) {
	is := is.New(t)

	is.True(strings.HasSuffix(xdg.Get(xdg.EnvDataHome, "test"), "test"))
}
