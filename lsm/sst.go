// SPDX-FileCopyrightText: 2023 Jon Lundy <jon@xuu.cc>
// SPDX-License-Identifier: BSD-3-Clause

// lsm -- Log Structured Merge-Tree
//
// This is a basic LSM tree using a SSTable optimized for append only writing. On disk data is organized into time ordered
// files of segments, containing reverse sorted keys. Each segment ends with a magic value `Souris\x01`, a 4byte hash, count of
// segment entries, and data length.

package lsm

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"io/fs"
	"sort"
)

var (
	magic      = reverse(append([]byte("Souris"), '\x01'))
	hash       = fnv.New32a
	hashLength = hash().Size()
	// segmentSize         = 2 ^ 16 // min 2^9 = 512b, max? 2^20 = 1M
	segmentFooterLength = len(magic) + hashLength + binary.MaxVarintLen32 + binary.MaxVarintLen32
)

type header struct {
	sig     []byte // 4Byte signature
	entries uint64 // count of entries in segment
	datalen uint64 // length of data
	headlen uint64 // length of header
	end     int64  // location of end of data/start of header (start of data is `end - datalen`)
}

// ReadHead parse header from a segment. reads from the end of slice of length segmentFooterLength
func ReadHead(data []byte) (*header, error) {
	if len(data) < len(magic)+6 {
		return nil, fmt.Errorf("%w: invalid size", ErrDecode)
	}

	if !bytes.Equal(data[len(data)-len(magic):], magic) {
		return nil, fmt.Errorf("%w: invalid header", ErrDecode)
	}

	head := make([]byte, 0, segmentFooterLength)
	head = reverse(append(head, data[max(0, len(data)-cap(head)-1):]...))
	size, s := binary.Uvarint(head[len(magic)+4:])
	length, i := binary.Uvarint(head[len(magic)+4+s:])

	return &header{
		sig:     head[len(magic) : len(magic)+4],
		entries: size,
		datalen: length,
		headlen: uint64(len(magic) + hashLength + s + i),
		end:     int64(len(data)),
	}, nil
}
func (h *header) Append(data []byte) []byte {

	length := len(data)
	data = append(data, h.sig...)
	data = binary.AppendUvarint(data, h.entries)
	data = binary.AppendUvarint(data, h.datalen)
	reverse(data[length:])

	return append(data, magic...)
}

var _ encoding.BinaryMarshaler = (*segment)(nil)
var _ encoding.BinaryUnmarshaler = (*segment)(nil)

var ErrDecode = errors.New("decode")

func reverse[T any](b []T) []T {
	l := len(b)
	for i := 0; i < l/2; i++ {
		b[i], b[l-i-1] = b[l-i-1], b[i]
	}
	return b
}

// func clone[T ~[]E, E any](e []E) []E {
// 	return append(e[0:0:0], e...)
// }

type entryBytes []byte

// KeyValue returns the parsed key and value from an entry
func (e entryBytes) KeyValue() ([]byte, uint64) {
	if len(e) < 2 {
		return nil, 0
	}
	head := reverse(append(e[0:0:0], e[max(0, len(e)-binary.MaxVarintLen64):]...))
	value, i := binary.Uvarint(head)
	return append(e[0:0:0], e[:len(e)-i]...), value
}

// NewKeyValue packed into an entry
func NewKeyValue(key []byte, val uint64) entryBytes {
	length := len(key)
	data := append(key[0:0:0], key...)
	data = binary.AppendUvarint(data, val)
	reverse(data[length:])

	return data
}

type listEntries []entryBytes

// WriteTo implements io.WriterTo.
func (lis *listEntries) WriteTo(wr io.Writer) (int64, error) {
	if lis == nil {
		return 0, nil
	}

	head := header{
		entries: uint64(len(*lis)),
	}
	h := hash()

	wr = io.MultiWriter(wr, h)

	var i int64
	for _, b := range *lis {
		j, err := wr.Write(b)
		i += int64(j)
		if err != nil {
			return i, err
		}

		j, err = wr.Write(reverse(binary.AppendUvarint(make([]byte, 0, binary.MaxVarintLen32), uint64(len(b)))))
		i += int64(j)
		if err != nil {
			return i, err
		}
	}
	head.datalen = uint64(i)
	head.sig = h.Sum(nil)

	b := head.Append([]byte{})
	j, err := wr.Write(b)
	i += int64(j)

	return i, err
}

var _ sort.Interface = listEntries{}

// Len implements sort.Interface.
func (lis listEntries) Len() int {
	return len(lis)
}

// Less implements sort.Interface.
func (lis listEntries) Less(i int, j int) bool {
	iname, _ := lis[i].KeyValue()
	jname, _ := lis[j].KeyValue()

	return bytes.Compare(iname, jname) < 0
}

// Swap implements sort.Interface.
func (lis listEntries) Swap(i int, j int) {
	lis[i], lis[j] = lis[j], lis[i]
}

type segmentReader struct {
	head *header
	rd   io.ReaderAt
}

// FirstEntry parses the first segment entry from the end of the segment
func (s *segmentReader) FirstEntry() (*entryBytes, error) {
	e, _, err := s.readEntryAt(-1)
	return e, err
}

func (s *segmentReader) VerifyHash() (bool, error) {
	h := hash()
	data := make([]byte, s.head.datalen)
	_, err := s.rd.ReadAt(data, s.head.end-int64(s.head.datalen))
	if err != nil {
		return false, err
	}
	_, err = h.Write(data)
	ok := bytes.Equal(h.Sum(nil), s.head.sig)

	return ok, err
}

// Find locates needle within a segment. if it cant find it will return the nearest key before needle.
func (s *segmentReader) Find(needle []byte, first bool) (*entryBytes, bool, error) {
	if s == nil {
		return nil, false, nil
	}
	e, pos, err := s.readEntryAt(-1)
	if err != nil {
		return nil, false, err
	}

	last := e
	found := false
	for pos > 0 {
		key, _ := e.KeyValue()
		switch bytes.Compare(key, needle) {
		case 1: // key=ccc, needle=bbb
			return last, found, nil
		case 0: // equal
			if first {
				return e, true, nil
			}
			found = true
			fallthrough
		case -1: // key=aaa, needle=bbb
			last = e
			e, pos, err = s.readEntryAt(pos)
			if err != nil {
				return nil, found, err
			}
		}
	}
	return last, found, nil
}
func (s *segmentReader) readEntryAt(pos int64) (*entryBytes, int64, error) {
	if pos < 0 {
		pos = s.head.end
	}
	head := make([]byte, binary.MaxVarintLen16)
	s.rd.ReadAt(head, pos-binary.MaxVarintLen16)
	length, hsize := binary.Uvarint(reverse(head))

	e := make(entryBytes, length)
	_, err := s.rd.ReadAt(e, pos-int64(length)-int64(hsize))

	return &e, pos - int64(length) - int64(hsize), err
}

type logFile struct {
	rd interface {
		io.ReaderAt
		io.WriterTo
	}
	segments []segmentReader

	fs.File
}

func ReadFile(fd fs.File) (*logFile, error) {
	l := &logFile{File: fd}

	stat, err := fd.Stat()
	if err != nil {
		return nil, err
	}

	eof := stat.Size()
	if rd, ok := fd.(interface {
		io.ReaderAt
		io.WriterTo
	}); ok {
		l.rd = rd

	} else {
		rd, err := io.ReadAll(fd)
		if err != nil {
			return nil, err
		}
		l.rd = bytes.NewReader(rd)
	}

	head := make([]byte, segmentFooterLength)
	for eof > 0 {
		_, err = l.rd.ReadAt(head, eof-int64(segmentFooterLength))
		if err != nil {
			return nil, err
		}

		s := segmentReader{
			rd: l.rd,
		}
		s.head, err = ReadHead(head)
		s.head.end = eof - int64(s.head.headlen)
		if err != nil {
			return nil, err
		}
		eof -= int64(s.head.datalen) + int64(s.head.headlen)
		l.segments = append(l.segments, s)
	}

	return l, nil
}

func (l *logFile) Count() int64 {
	return int64(len(l.segments))
}
func (l *logFile) LoadSegment(pos int64) (*segmentBytes, error) {
	if pos < 0 {
		pos = int64(len(l.segments) - 1)
	}
	if pos > int64(len(l.segments)-1) {
		return nil, ErrDecode
	}
	s := l.segments[pos]

	b := make([]byte, s.head.datalen+s.head.headlen)
	_, err := l.rd.ReadAt(b, s.head.end-int64(len(b)))
	if err != nil {
		return nil, err
	}

	return &segmentBytes{b, -1}, nil
}
func (l *logFile) Find(needle []byte, first bool) (*entryBytes, bool, error) {
	var cur, last segmentReader

	for _, s := range l.segments {
		cur = s
		e, err := cur.FirstEntry()
		if err != nil {
			return nil, false, err
		}
		k, _ := e.KeyValue()

		if first && bytes.Equal(k, needle) {
			break
		}
		if first && bytes.Compare(k, needle) > 0 {
			e, ok, err := cur.Find(needle, first)
			if ok || err != nil{
				return e, ok, err
			}
			break
		}
		if !first && bytes.Compare(k, needle) > 0 {
			break
		}
		last = s
	}

	e, ok, err := last.Find(needle, first)
	if ok || err != nil{
		return e, ok, err
	}
	// if by mistake it was not found in the last.. check the next segment.
	return cur.Find(needle, first)
}
func (l *logFile) WriteTo(w io.Writer) (int64, error) {
	return l.rd.WriteTo(w)
}

type segmentBytes struct {
	b   []byte
	pos int
}

type dataset struct {
	rd    io.ReaderAt
	files []logFile

	fs.FS
}

func ReadDataset(fd fs.FS) (*dataset, error) {
	panic("not implemented")
}
