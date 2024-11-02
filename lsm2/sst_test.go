package lsm2

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/docopt/docopt-go"
	"github.com/matryer/is"
)

func TestAppend(t *testing.T) {
	type test struct {
		name string
		in   [][]io.Reader
		enc  string
		out  [][]byte
		rev  [][]byte
	}
	tests := []test{
		{
			"nil reader",
			nil,
			"U291ci5pcwAAAwACAA",
			[][]byte{},
			[][]byte{},
		},
		{
			"single reader",
			[][]io.Reader{
				{
					bytes.NewBuffer([]byte{1, 2, 3, 4})}},
			"U291ci5pcwAAE756XndRZXhdAAYBAgMEAQQBAhA",
			[][]byte{{1, 2, 3, 4}},
			[][]byte{{1, 2, 3, 4}}},
		{
			"multiple readers",
			[][]io.Reader{
				{
					bytes.NewBuffer([]byte{1, 2, 3, 4}),
					bytes.NewBuffer([]byte{5, 6, 7, 8})}},
			"U291ci5pcwAAI756XndRZXhdAAYBAgMEAQRhQyZWDDn5BQAGBQYHCAEEAgIg",
			[][]byte{{1, 2, 3, 4}, {5, 6, 7, 8}},
			[][]byte{{5, 6, 7, 8}, {1, 2, 3, 4}}},
		{
			"multiple commit",
			[][]io.Reader{
				{
					bytes.NewBuffer([]byte{1, 2, 3, 4})},
				{
					bytes.NewBuffer([]byte{5, 6, 7, 8})}},
			"U291ci5pcwAAJ756XndRZXhdAAYBAgMEAQQBAhBhQyZWDDn5BQAGBQYHCAEEEAIDIA",
			[][]byte{{1, 2, 3, 4}, {5, 6, 7, 8}},
			[][]byte{{5, 6, 7, 8}, {1, 2, 3, 4}}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			buf := &buffer{}

			buffers := 0

			if len(test.in) == 0 {
				err := WriteLogFile(buf)
				is.NoErr(err)
			}
			for i, in := range test.in {
				buffers += len(in)

				if i == 0 {
					err := WriteLogFile(buf, in...)
					is.NoErr(err)
				} else {
					err := AppendLogFile(buf, in...)
					is.NoErr(err)
				}
			}

			is.Equal(base64.RawStdEncoding.EncodeToString(buf.Bytes()), test.enc)

			files, err := ReadLogFile(bytes.NewReader(buf.Bytes()))
			is.NoErr(err)

			i := 0
			for fp := range iterOne(files.Iter()) {
				buf, err := io.ReadAll(fp)
				is.NoErr(err)

				is.Equal(buf, test.out[i])
				i++
			}
			is.NoErr(files.Err)
			is.Equal(i, buffers)

			// i = 0
			// for fp := range iterOne(files.Rev()) {
			// 	buf, err := io.ReadAll(fp)
			// 	is.NoErr(err)

			// 	is.Equal(buf, test.rev[i])
			// 	i++
			// }
			// is.NoErr(files.Err)
			// is.Equal(i, buffers)

		})
	}
}

func TestArgs(t *testing.T) {
	is := is.New(t)
	usage := `Usage: lsm2 create <archive> <files>...
	`
	opts, err := docopt.ParseArgs(usage, []string{"create", "archive", "file1", "file2"}, "1.0")
	is.NoErr(err)

	args := struct {
		Create  bool     `docopt:"create"`
		Archive string   `docopt:"<archive>"`
		Files   []string `docopt:"<files>"`
	}{}
	err = opts.Bind(&args)
	is.NoErr(err)
	fmt.Println(args)

}

type buffer struct {
	buf []byte
}

// Bytes returns the underlying byte slice of the bufferWriterAt.
func (b *buffer) Bytes() []byte {
	return b.buf
}

// WriteAt implements io.WriterAt. It appends data to the internal buffer
// if the offset is beyond the current length of the buffer. It will
// return an error if the offset is negative.
func (b *buffer) WriteAt(data []byte, offset int64) (written int, err error) {
	if offset < 0 {
		return 0, errors.New("negative offset")
	}

	currentLength := int64(len(b.buf))
	if currentLength < offset+int64(len(data)) {
		b.buf = append(b.buf, make([]byte, offset+int64(len(data))-currentLength)...)
	}

	written = copy(b.buf[offset:], data)
	return
}

func (b *buffer) ReadAt(data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("negative offset")
	}

	if offset > int64(len(b.buf)) || len(b.buf[offset:]) < len(data) {
		return copy(data, b.buf[offset:]), io.EOF
	}

	return copy(data, b.buf[offset:]), nil
}
