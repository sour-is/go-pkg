package main

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"os"

	"github.com/docopt/docopt-go"
	"go.sour.is/pkg/lsm2"
)

var usage = `
Usage: 
  lsm2 create <archive> <files>...
  lsm2 append <archive> <files>...
  lsm2 read <archive> <index>`

type args struct {
	Create  bool
	Append  bool
	Read    bool

	Archive string   `docopt:"<archive>"`
	Files   []string `docopt:"<files>"`
	Index   int64  `docopt:"<index>"`
}

func main() {
	opts, err := docopt.ParseDoc(usage)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println(opts, must(opts.Bool("create")))
	console := console{os.Stdin, os.Stdout, os.Stderr}
	args := args{}
	err = opts.Bind(&args)
	fmt.Println(err)
	run(console, args)
}

type console struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer
}

func (c console) Write(b []byte) (int, error) {
	return c.Stdout.Write(b)
}

func run(console console,a args) error {
	fmt.Fprintln(console, "lsm")
	switch {
	case a.Create:
		f, err := os.OpenFile(a.Archive, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer f.Close()

		return lsm2.WriteLogFile(f, fileReaders(a.Files))
	case a.Append:
		f, err := os.OpenFile(a.Archive, os.O_RDWR, 0644)
		if err != nil {
			return err
		}
		defer f.Close()

		return lsm2.AppendLogFile(f, fileReaders(a.Files))
	case a.Read:
		fmt.Fprintln(console, "reading", a.Archive)

		f, err := os.Open(a.Archive)
		if err != nil {
			return err
		}
		defer f.Close()	
		lg, err := lsm2.ReadLogFile(f)
		if err != nil {
			return err
		}
		for i, rd := range lg.Iter() {
			fmt.Fprintf(console, "=========================\n%d:\n", i)
			io.Copy(console, rd)
			fmt.Fprintln(console, "=========================")
		}
		if lg.Err != nil {
			return lg.Err
		}
		return nil
	default:
		return errors.New("unknown command")
	}
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

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
