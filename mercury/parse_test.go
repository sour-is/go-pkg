package mercury_test

import (
	"strings"
	"testing"

	"github.com/matryer/is"
	"go.sour.is/pkg/mercury"
)

func TestParseText(t *testing.T) {
	is := is.New(t)
	sm, err := mercury.ParseText(strings.NewReader(`
@test.sign
key   :value1
-----BEGIN SSH SIGNATURE-----
U1NIU0lHAAAAAQAAADMAAAALc3NoLWVkMjU1MTkAAAAgZ+OuJYdd3UiUbyBuO1RlsQR20a
Qm5mKneuMxRjGo3zkAAAAEZmlsZQAAAAAAAAAGc2hhNTEyAAAAUwAAAAtzc2gtZWQyNTUx
OQAAAED8T4C6WILXYZ1KxqDIlVhlrAEjr1Vc+tn8ypcVM3bN7iOexVvuUuvm90nr8eEwKU
acrdDxmq2S+oysQbK+pMUE
-----END SSH SIGNATURE-----
`))
	is.NoErr(err)
	for _, c := range sm {
		is.Equal(len(c.Trailer), 6)
	}

}
