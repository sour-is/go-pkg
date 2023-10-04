package env_test

import (
	"os"
	"testing"

	"github.com/matryer/is"
	"go.sour.is/pkg/env"
)

func TestDefault(t *testing.T) {
	is := is.New(t)

	os.Setenv("SOME_VALUE", "")

	v := env.Default("SOME_VALUE ", "default")
	is.Equal(v, "default")

	os.Setenv("SOME_VALUE", "value")

	v = env.Default("SOME_VALUE", "default")
	is.Equal(v, "value")
}

func TestSecret(t *testing.T) {
	is := is.New(t)

	os.Setenv("SOME_VALUE", "")

	v := env.Secret("SOME_VALUE ", "")
	is.Equal(v.Secret(), "")
	is.Equal(v.String(), "(nil)")

	os.Setenv("SOME_VALUE", "value")

	v = env.Secret("SOME_VALUE", "default")
	is.Equal(v.Secret(), "value")
	is.Equal(v.String(), "***")
}
