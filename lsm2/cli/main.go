package main

import (
	"errors"
	"fmt"
	"io"
	"iter"
	"os"
	"path/filepath"

	"github.com/docopt/docopt-go"
	"go.sour.is/pkg/lsm2"
)

var usage = `
Usage: lsm2 create <archive> <files>...`

type args struct {
	Create  bool
	Archive string   `docopt:"<archive>"`
	Files   []string `docopt:"<files>"`
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

func run(console console, args args) error {
	switch {
	case args.Create:
		fmt.Fprintf(console, "creating %s from %v\n", filepath.Base(args.Archive), args.Files)
		out, err := os.OpenFile(args.Archive, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer out.Close()

		filesWritten := 0
		defer func() { fmt.Fprintln(console, "wrote", filesWritten, "files") }()

		return lsm2.WriteIter(out, iter.Seq[io.Reader](func(yield func(io.Reader) bool) {
			for _, name := range args.Files {
				f, err := os.Open(name)
				if err != nil {
					continue
				}
				filesWritten++
				if !yield(f) {
					f.Close()
					return
				}
				f.Close()
			}
		}))
	default:
		return errors.New("unknown command")
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}
