package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"iter"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/docopt/docopt-go"
	"go.sour.is/pkg/lsm"
)

var usage = `
Usage: 
  lsm create <archive> <files>...
  lsm append <archive> <files>...
  lsm read <archive> [<start> [<end>]]
  lsm serve <archive>
  lsm client <archive> [<start> [<end>]]`

type args struct {
	Create bool
	Append bool
	Read   bool
	Serve  bool
	Client bool

	Archive string   `docopt:"<archive>"`
	Files   []string `docopt:"<files>"`
	Start   int64    `docopt:"<start>"`
	End     int64    `docopt:"<end>"`
}

func main() {
	opts, err := docopt.ParseDoc(usage)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	args := args{}
	err = opts.Bind(&args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	err = run(Console, args)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type console struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}
var Console = console{os.Stdin, os.Stdout, os.Stderr}

func (c console) Write(b []byte) (int, error) {
	return c.Stdout.Write(b)
}

func run(console console, a args) error {
	fmt.Fprintln(console, "lsm")
	switch {
	case a.Create:
		f, err := os.OpenFile(a.Archive, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer f.Close()

		return lsm.WriteLogFile(f, fileReaders(a.Files))
	case a.Append:
		f, err := os.OpenFile(a.Archive, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		defer f.Close()

		return lsm.AppendLogFile(f, fileReaders(a.Files))
	case a.Read:
		fmt.Fprintln(console, "reading", a.Archive)

		f, err := os.Open(a.Archive)
		if err != nil {
			return err
		}
		defer f.Close()
		return readContent(f, console, a.Start, a.End)
	case a.Serve:
		fmt.Fprintln(console, "serving", a.Archive)
		b, err := base64.RawStdEncoding.DecodeString(a.Archive)
		now := time.Now()
		if err != nil {
			return err
		}
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			http.ServeContent(w, r, "", now, bytes.NewReader(b))
		})
		return http.ListenAndServe(":8080", nil)
	case a.Client:
		r, err := OpenHttpReader(context.Background(), a.Archive, 0)
		if err != nil {
			return err
		}
		defer r.Close()
		defer func() {fmt.Println("bytes read", r.bytesRead)}()
		return readContent(r, console, a.Start, a.End)
	}
	return errors.New("unknown command")
}

func readContent(r io.ReaderAt, console console, start, end int64) error {
	lg, err := lsm.ReadLogFile(r)
	if err != nil {
		return err
	}
	for bi, rd := range lg.Iter(uint64(start)) {
		if end > 0 && int64(bi.Index) >= end {
			break
		}
		fmt.Fprintf(console, "=========================\n%+v:\n", bi)
		wr := base64.NewEncoder(base64.RawStdEncoding, console)
		io.Copy(wr, rd)
		fmt.Fprintln(console, "\n=========================")
	}
	if lg.Err != nil {
		return lg.Err
	}
	for bi, rd := range lg.Rev(lg.Count()) {
		if end > 0 && int64(bi.Index) >= end {
			break
		}
		fmt.Fprintf(console, "=========================\n%+v:\n", bi)
		wr := base64.NewEncoder(base64.RawStdEncoding, console)
		io.Copy(wr, rd)
		fmt.Fprintln(console, "\n=========================")
	}

	return lg.Err
}


func fileReaders(names []string) iter.Seq[io.Reader] {
	return iter.Seq[io.Reader](func(yield func(io.Reader) bool) {
		for _, name := range names {
			f, err := os.Open(name)
			if err != nil {
				continue
			}
			if !yield(f) {
				f.Close()
				return
			}
			f.Close()
		}
	})
}

type HttpReader struct {
	ctx     context.Context
	uri     url.URL
	tmpfile *os.File
	pos     int64
	end     int64
	bytesRead int
}

func OpenHttpReader(ctx context.Context, uri string, end int64) (*HttpReader, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, err
	}
	return &HttpReader{ctx: ctx, uri: *u, end: end}, nil
}

func (r *HttpReader) Read(p []byte) (int, error) {
	n, err := r.ReadAt(p, r.pos)
	if err != nil {
		return n, err
	}
	r.pos += int64(n)
	r.bytesRead += n
	return n, nil
}

func (r *HttpReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		r.pos = offset
	case io.SeekCurrent:
		r.pos += offset
	case io.SeekEnd:
		r.pos = r.end + offset
	}
	return r.pos, nil
}

func (r *HttpReader) Close() error {
	r.ctx.Done()
	return nil
}

// ReadAt implements io.ReaderAt. It reads data from the internal buffer starting
// from the specified offset and writes it into the provided data slice. If the
// offset is negative, it returns an error. If the requested read extends beyond
// the buffer's length, it returns the data read so far along with an io.EOF error.
func (r *HttpReader) ReadAt(data []byte, offset int64) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}

	if offset < 0 {
		return 0, errors.New("negative offset")
	}

	if r.end > 0 && offset > r.end {
		return 0, io.EOF
	}

	dlen := len(data) + int(offset)

	if r.end > 0 && r.end+int64(dlen) > r.end {
		dlen = int(r.end)
	}

	end := ""
	if r.end > 0 {
		end = fmt.Sprintf("/%d", r.end)
	}
	req, err := http.NewRequestWithContext(r.ctx, "GET", r.uri.String(), nil)
	if err != nil {
		return 0, err
	}

	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d%s", offset, dlen, end))

	fmt.Fprintln(Console.Stderr, req)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		fmt.Fprintln(Console.Stderr, "requested range not satisfiable")
		return 0, io.EOF
	}

	if resp.StatusCode == http.StatusOK {

		r.tmpfile, err = os.CreateTemp("", "httpReader")
		if err != nil {
			return 0, err
		}
		defer os.Remove(r.tmpfile.Name())
		n, err := io.Copy(r.tmpfile, resp.Body)
		if err != nil {
			return 0, err
		}
		r.bytesRead += int(n)

		defer fmt.Fprintln(Console.Stderr, "wrote ", n, " bytes to ", r.tmpfile.Name())
		resp.Body.Close()
		r.tmpfile.Seek(offset, 0)
		return io.ReadFull(r.tmpfile, data)
	}

	n, err := io.ReadFull(resp.Body, data)
	if n == 0 && err != nil {
		return n, err
	}
	r.bytesRead += n
	defer fmt.Fprintln(Console.Stderr, "read ", n, " bytes")
	return n, nil
}
