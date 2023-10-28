package lsm

import (
	"io/fs"
	"testing"

	"github.com/matryer/is"
)

func TestEncoding(t *testing.T) {
	is := is.New(t)

	data := segment{entries: entries{
		{"key-1", 1},
		{"key-2", 2},
		{"key-3", 3},
		{"longerkey-4", 65535},
	}}

	b, err := data.MarshalBinary()
	is.NoErr(err)

	var got segment
	err = got.UnmarshalBinary(b)
	is.NoErr(err)

	is.Equal(data, got)
}

func TestReverse(t *testing.T) {
	is := is.New(t)

	got := []byte("gnirts a si siht")
	reverse(got)

	is.Equal(got, []byte("this is a string"))

	got = []byte("!gnirts a si siht")
	reverse(got)

	is.Equal(got, []byte("this is a string!"))
}

func TestFile(t *testing.T) {
	is := is.New(t)

	f := basicFile(t)

	sf, err := ReadFile(f)
	is.NoErr(err)

	is.Equal(len(sf.segments), 3)
}

func basicFile(t *testing.T) fs.File {
	t.Helper()

	data := segment{entries: entries{
		{"key-1", 1},
		{"key-2", 2},
		{"key-3", 3},
		{"longerkey-4", 65535},
	}}

	b, err := data.MarshalBinary()
	if err != nil {
		t.Error(err)
	}

	return NewFile(b, b, b)
}
