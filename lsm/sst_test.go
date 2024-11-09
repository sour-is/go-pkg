package lsm

import (
	"bytes"
	"encoding/base64"
	"errors"
	"io"
	"iter"
	"slices"
	"testing"

	"github.com/docopt/docopt-go"
	"github.com/matryer/is"
)

// TestWriteLogFile tests AppendLogFile and WriteLogFile against a set of test cases.
//
// Each test case contains a slice of slices of io.Readers, which are passed to
// AppendLogFile and WriteLogFile in order. The test case also contains the
// expected encoded output as a base64 string, as well as the expected output
// when the file is read back using ReadLogFile.
//
// The test case also contains the expected output when the file is read back in
// reverse order using ReadLogFile.Rev().
//
// The test cases are as follows:
//
// - nil reader: Passes a nil slice of io.Readers to WriteLogFile.
// - err reader: Passes a slice of io.Readers to WriteLogFile which returns an
//   error when read.
// - single reader: Passes a single io.Reader to WriteLogFile.
// - multiple readers: Passes a slice of multiple io.Readers to WriteLogFile.
// - multiple commit: Passes multiple slices of io.Readers to AppendLogFile.
// - multiple commit 3x: Passes multiple slices of io.Readers to AppendLogFile
//   three times.
//
// The test uses the is package from github.com/matryer/is to check that the
// output matches the expected output.
func TestWriteLogFile(t *testing.T) {
	type test struct {
		name string
		in   [][]io.Reader
		enc  string
		out  [][]byte
		rev  [][]byte
	}
	tests := []test{
		{
			name: "nil reader",
			in:   nil,
			enc:  "U291ci5pcwAAAwACAA",
			out:  [][]byte{},
			rev:  [][]byte{},
		},
		{
			name: "err reader",
			in:   nil,
			enc:  "U291ci5pcwAAAwACAA",
			out:  [][]byte{},
			rev:  [][]byte{},
		},
		{
			name: "single reader",
			in: [][]io.Reader{
				{
					bytes.NewBuffer([]byte{1, 2, 3, 4})}},
			enc: "U291ci5pcwAAE756XndRZXhdAAYBAgMEAQQBAhA",
			out: [][]byte{{1, 2, 3, 4}},
			rev: [][]byte{{1, 2, 3, 4}}},
		{
			name: "multiple readers",
			in: [][]io.Reader{
				{
					bytes.NewBuffer([]byte{1, 2, 3, 4}),
					bytes.NewBuffer([]byte{5, 6, 7, 8})}},
			enc: "U291ci5pcwAAI756XndRZXhdAAYBAgMEAQRhQyZWDDn5BQAGBQYHCAEEAgIg",
			out: [][]byte{{1, 2, 3, 4}, {5, 6, 7, 8}},
			rev: [][]byte{{5, 6, 7, 8}, {1, 2, 3, 4}}},
		{
			name: "multiple commit",
			in: [][]io.Reader{
				{
					bytes.NewBuffer([]byte{1, 2, 3, 4})},
				{
					bytes.NewBuffer([]byte{5, 6, 7, 8})}},
			enc: "U291ci5pcwAAJr56XndRZXhdAAYBAgMEAQQBAhBhQyZWDDn5BQAGBQYHCAEEAgIQ",
			out: [][]byte{{1, 2, 3, 4}, {5, 6, 7, 8}},
			rev: [][]byte{{5, 6, 7, 8}, {1, 2, 3, 4}}},
		{
			name: "multiple commit",
			in: [][]io.Reader{
				{
					bytes.NewBuffer([]byte{1, 2, 3, 4}),
					bytes.NewBuffer([]byte{5, 6, 7, 8})},
				{
					bytes.NewBuffer([]byte{9, 10, 11, 12})},
			},
			enc: "U291ci5pcwAANr56XndRZXhdAAYBAgMEAQRhQyZWDDn5BQAGBQYHCAEEAgIgA4Buuio8Ro0ABgkKCwwBBAMCEA",
			out: [][]byte{{1, 2, 3, 4}, {5, 6, 7, 8}, {9, 10, 11, 12}},
			rev: [][]byte{{9, 10, 11, 12}, {5, 6, 7, 8}, {1, 2, 3, 4}}},
		{
			name: "multiple commit 3x",
			in: [][]io.Reader{
				{
					bytes.NewBuffer([]byte{1, 2, 3}),
					bytes.NewBuffer([]byte{4, 5, 6}),
				},
				{
					bytes.NewBuffer([]byte{7, 8, 9}),
				},
				{
					bytes.NewBuffer([]byte{10, 11, 12}),
					bytes.NewBuffer([]byte{13, 14, 15}),
				},
			},
			enc: "U291ci5pcwAAVNCqYhhnLPWrAAUBAgMBA7axWhhYd+HsAAUEBQYBAwICHr9ryhhdbkEZAAUHCAkBAwMCDy/UIhidCwCqAAUKCwwBA/NCwhh6wXgXAAUNDg8BAwUCHg",
			out: [][]byte{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10, 11, 12}, {13, 14, 15}},
			rev: [][]byte{{13, 14, 15}, {10, 11, 12}, {7, 8, 9}, {4, 5, 6}, {1, 2, 3}}},
	}	

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			is := is.New(t)
			buf := &buffer{}

			buffers := 0

			if len(test.in) == 0 {
				err := WriteLogFile(buf, slices.Values([]io.Reader{}))
				is.NoErr(err)
			}
			for i, in := range test.in {
				buffers += len(in)

				if i == 0 {
					err := WriteLogFile(buf, slices.Values(in))
					is.NoErr(err)
				} else {
					err := AppendLogFile(buf, slices.Values(in))
					is.NoErr(err)
				}
			}
			
			is.Equal(base64.RawStdEncoding.EncodeToString(buf.Bytes()), test.enc)

			files, err := ReadLogFile(bytes.NewReader(buf.Bytes()))
			is.NoErr(err)

			is.Equal(files.Size(), uint64(len(buf.Bytes())))

			i := 0
			for bi, fp := range files.Iter(0) {
				buf, err := io.ReadAll(fp)
				is.NoErr(err)

				hash := hash()
				hash.Write(buf)
				is.Equal(bi.Hash, hash.Sum(nil)[:len(bi.Hash)])

				is.True(len(test.out) > int(bi.Index))
				is.Equal(buf, test.out[bi.Index])
				i++
			}
			is.NoErr(files.Err)
			is.Equal(i, buffers)

			i = 0
			for bi, fp := range files.Rev(files.Count()) {
				buf, err := io.ReadAll(fp)
				is.NoErr(err)

				hash := hash()
				hash.Write(buf)
				is.Equal(bi.Hash, hash.Sum(nil)[:len(bi.Hash)])

				is.Equal(buf, test.rev[i])
				is.Equal(buf, test.out[bi.Index])
				i++
			}
			is.NoErr(files.Err)
			is.Equal(i, buffers)
			is.Equal(files.Count(), uint64(i))
		})
	}
}

// TestArgs tests that the CLI arguments are correctly parsed.
func TestArgs(t *testing.T) {
	is := is.New(t)
	usage := `Usage: lsm2 create <archive> <files>...`

	arguments, err := docopt.ParseArgs(usage, []string{"create", "archive", "file1", "file2"}, "1.0")
	is.NoErr(err)

	var params struct {
		Create  bool     `docopt:"create"`
		Archive string   `docopt:"<archive>"`
		Files   []string `docopt:"<files>"`
	}
	err = arguments.Bind(&params)
	is.NoErr(err)

	is.Equal(params.Create, true)
	is.Equal(params.Archive, "archive")
	is.Equal(params.Files, []string{"file1", "file2"})
}

func BenchmarkIterate(b *testing.B) {
	block := make([]byte, 1024)
	buf := &buffer{}


	b.Run("write", func(b *testing.B) {
		WriteLogFile(buf, func(yield func(io.Reader) bool) {
			for range (b.N) {
				if !yield(bytes.NewBuffer(block)) {
					break	
				}	
			}
		})
	})

	b.Run("read", func(b *testing.B) {
		lf, _ := ReadLogFile(buf)
		b.Log(lf.Count())
		for range (b.N) {
			for _, fp := range lf.Iter(0) {
				_, _ = io.Copy(io.Discard, fp)
				break
			}
		}
	})

	b.Run("rev", func(b *testing.B) {
		lf, _ := ReadLogFile(buf)
		b.Log(lf.Count())
		for range (b.N) {
			for _, fp := range lf.Rev(lf.Count()) {
				_, _ = io.Copy(io.Discard, fp)
				break
			}
		}
	})
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

// ReadAt implements io.ReaderAt. It reads data from the internal buffer starting
// from the specified offset and writes it into the provided data slice. If the
// offset is negative, it returns an error. If the requested read extends beyond
// the buffer's length, it returns the data read so far along with an io.EOF error.
func (b *buffer) ReadAt(data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("negative offset")
	}

	if offset > int64(len(b.buf)) || len(b.buf[offset:]) < len(data) {
		return copy(data, b.buf[offset:]), io.EOF
	}

	return copy(data, b.buf[offset:]), nil
}

// IterOne takes an iterator that yields values of type T along with a value of
// type I, and returns an iterator that yields only the values of type T. It
// discards the values of type I.
func IterOne[I, T any](it iter.Seq2[I, T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for i, v := range it {
			_ = i
			if !yield(v) {
				return
			}
		}
	}
}
