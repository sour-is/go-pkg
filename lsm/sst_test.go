// SPDX-FileCopyrightText: 2023 Jon Lundy <jon@xuu.cc>
// SPDX-License-Identifier: BSD-3-Clause

package lsm

import (
	"bytes"
	crand "crypto/rand"
	"encoding/base64"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/matryer/is"
)

func TestLargeFile(t *testing.T) {
	is := is.New(t)

	segCount := 4098

	f := randFile(t, 2_000_000, segCount)

	sf, err := ReadFile(f)
	is.NoErr(err)

	is.True(len(sf.segments) <= segCount)
	var needle []byte
	for i, s := range sf.segments {
		e, err := s.FirstEntry()
		is.NoErr(err)
		k, v := e.KeyValue()
		needle = k
		t.Logf("Segment-%d: %s = %d", i, k, v)
	}
	t.Log(f.Stat())

	tt, ok, err := sf.Find(needle, true)
	is.NoErr(err)
	is.True(ok)
	key, val := tt.KeyValue()
	t.Log(string(key), val)

	tt, ok, err = sf.Find([]byte("needle"), false)
	is.NoErr(err)
	is.True(!ok)
	key, val = tt.KeyValue()
	t.Log(string(key), val)

	tt, ok, err = sf.Find([]byte{'\xff'}, false)
	is.NoErr(err)
	is.True(!ok)
	key, val = tt.KeyValue()
	t.Log(string(key), val)
}

func TestLargeFileDisk(t *testing.T) {
	is := is.New(t)

	segCount := 4098

	t.Log("generate large file")
	f := randFile(t, 2_000_000, segCount)

	fd, err := os.CreateTemp("", "sst*")
	is.NoErr(err)
	defer func() { t.Log("cleanup:", fd.Name()); fd.Close(); os.Remove(fd.Name()) }()

	t.Log("write file:", fd.Name())
	_, err = io.Copy(fd, f)
	is.NoErr(err)
	fd.Seek(0, 0)

	sf, err := ReadFile(fd)
	is.NoErr(err)

	is.True(len(sf.segments) <= segCount)
	var needle []byte
	for i, s := range sf.segments {
		e, err := s.FirstEntry()
		is.NoErr(err)
		k, v := e.KeyValue()
		needle = k

		ok, err := s.VerifyHash()
		is.NoErr(err)

		t.Logf("Segment-%d: %s = %d %t", i, k, v, ok)
		is.True(ok)
	}
	t.Log(f.Stat())

	tt, ok, err := sf.Find(needle, false)
	is.NoErr(err)
	is.True(ok)
	key, val := tt.KeyValue()
	t.Log(string(key), val)

	tt, ok, err = sf.Find([]byte("needle"), false)
	is.NoErr(err)
	is.True(!ok)
	key, val = tt.KeyValue()
	t.Log(string(key), val)

	tt, ok, err = sf.Find([]byte{'\xff'}, false)
	is.NoErr(err)
	is.True(!ok)
	key, val = tt.KeyValue()
	t.Log(string(key), val)
}

func BenchmarkLargeFile(b *testing.B) {
	segCount := 4098 / 4
	f := randFile(b, 2_000_000, segCount)

	sf, err := ReadFile(f)
	if err != nil {
		b.Error(err)
	}
	key := make([]byte, 5)
	keys := make([][]byte, b.N)
	for i := range keys {
		_, err = crand.Read(key)
		if err != nil {
			b.Error(err)
		}
		keys[i] = []byte(base64.RawURLEncoding.EncodeToString(key))
	}
	b.Log("ready", b.N)
	b.ResetTimer()
	okays := 0
	each := b.N / 10
	for n := 0; n < b.N; n++ {
		if each > 0 && n%each == 0 {
			b.Log(n)
		}
		_, ok, err := sf.Find(keys[n], false)
		if err != nil {
			b.Error(err)
		}
		if ok {
			okays++
		}
	}
	b.Log("okays=", b.N, okays)
}

// TestFindRange is an initial range find for start and stop of a range of needles.
// TODO: start the second query from where the first left off. Use an iterator?
func TestFindRange(t *testing.T) {
	is := is.New(t)

	f := basicFile(t, 
		entries{
			{"AD", 5},
			{"AC", 5},
			{"AB", 4},
			{"AB", 3},
		},
		entries{
			{"AB", 2},
			{"AA", 1},
		},
	)
	sf, err := ReadFile(f)
	is.NoErr(err)

	var ok bool
	var first, last  *entryBytes

	first, ok, err = sf.Find([]byte("AB"), true)
	is.NoErr(err)

	key, val := first.KeyValue()
	t.Log(string(key), val)

	is.True(ok)
	is.Equal(key, []byte("AB"))
	is.Equal(val, uint64(2))

	last, ok, err = sf.Find([]byte("AB"), false)
	is.NoErr(err)

	key, val = last.KeyValue()
	t.Log(string(key), val)

	is.True(ok)
	is.Equal(key, []byte("AB"))
	is.Equal(val, uint64(4))


	last, ok, err = sf.Find([]byte("AC"), false)
	is.NoErr(err)

	key, val = last.KeyValue()
	t.Log(string(key), val)

	is.True(ok)
	is.Equal(key, []byte("AC"))
	is.Equal(val, uint64(5))
}

func randFile(t interface {
	Helper()
	Error(...any)
}, size int, segments int) fs.File {
	t.Helper()

	lis := make(listEntries, size)
	for i := range lis {
		key := make([]byte, 5)
		_, err := crand.Read(key)
		if err != nil {
			t.Error(err)
		}
		key = []byte(base64.RawURLEncoding.EncodeToString(key))
		// key := []byte(fmt.Sprintf("key-%05d", i))

		lis[i] = NewKeyValue(key, rand.Uint64()%16_777_216)
	}

	sort.Sort(sort.Reverse(&lis))
	each := size / segments
	if size%segments != 0 {
		each++
	}
	split := make([]listEntries, segments)

	for i := range split {
		if (i+1)*each > len(lis) {
			split[i] = lis[i*each : i*each+len(lis[i*each:])]
			split = split[:i+1]
			break
		}
		split[i] = lis[i*each : (i+1)*each]
	}

	var b bytes.Buffer
	for _, s := range split {
		s.WriteTo(&b)
	}

	return NewFile(b.Bytes())
}

type fakeStat struct {
	size int64
}

// IsDir implements fs.FileInfo.
func (*fakeStat) IsDir() bool {
	panic("unimplemented")
}

// ModTime implements fs.FileInfo.
func (*fakeStat) ModTime() time.Time {
	panic("unimplemented")
}

// Mode implements fs.FileInfo.
func (*fakeStat) Mode() fs.FileMode {
	panic("unimplemented")
}

// Name implements fs.FileInfo.
func (*fakeStat) Name() string {
	panic("unimplemented")
}

// Size implements fs.FileInfo.
func (s *fakeStat) Size() int64 {
	return s.size
}

// Sys implements fs.FileInfo.
func (*fakeStat) Sys() any {
	panic("unimplemented")
}

var _ fs.FileInfo = (*fakeStat)(nil)

type rd interface {
	io.ReaderAt
	io.Reader
}
type fakeFile struct {
	stat func() fs.FileInfo

	rd
}

func (fakeFile) Close() error                 { return nil }
func (f fakeFile) Stat() (fs.FileInfo, error) { return f.stat(), nil }

func NewFile(b ...[]byte) fs.File {
	in := bytes.Join(b, nil)
	rd := bytes.NewReader(in)
	size := int64(len(in))
	return &fakeFile{stat: func() fs.FileInfo { return &fakeStat{size: size} }, rd: rd}
}
func NewFileFromReader(rd *bytes.Reader) fs.File {
	return &fakeFile{stat: func() fs.FileInfo { return &fakeStat{size: int64(rd.Len())} }, rd: rd}
}

type fakeFS struct {
	files map[string]*fakeFile
	mu    sync.RWMutex
}

// Open implements fs.FS.
func (f *fakeFS) Open(name string) (fs.File, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if file, ok := f.files[name]; ok {
		return file, nil
	}

	return nil, fs.ErrNotExist
}

var _ fs.FS = (*fakeFS)(nil)
