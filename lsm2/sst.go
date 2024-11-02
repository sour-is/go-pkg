package lsm2

// [Sour.is|size] [size|hash][data][hash|flag|size]... [prev|count|flag|size]

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"iter"
)

const (
	TypeUnknown uint64 = iota
	TypeSegment
	TypeCommit
	TypePrevCommit

	headerSize = 10

	maxCommitSize = 4 * binary.MaxVarintLen64
	minCommitSize = 3

	maxBlockSize = 2 * binary.MaxVarintLen64
	minBlockSize = 2
)

var (
	Magic   = [10]byte([]byte("Sour.is\x00\x00\x00"))
	Version = uint8(1)
	hash    = fnv.New64a

	ErrDecode = errors.New("decode")
)

type header struct {
	end   uint64
	extra []byte
}

func (h *header) UnmarshalBinary(data []byte) error {
	if len(data) != 10 {
		return fmt.Errorf("%w: bad data", ErrDecode)
	}
	h.extra = append(h.extra, data...)

	var n int
	h.end, n = binary.Uvarint(h.extra)
	reverse(h.extra)
	h.extra = h.extra[:len(h.extra)-n]

	return nil
}

// Commit1: [magic>|<end]{10} ... [<count][<size][<flag]{3..30}
//                 +---------|--------------------------------> end = seek to end of file
//                       <---|-------------+                    size = seek to magic header
//                       <---|-------------+10                  size + 10 = seek to start of file
//          <-----------------------------T+10----------------> 10 + size + trailer = full file size

// Commit2: [magic>|<end]{10} ... [<count][<size][<flag]{3..30} ... [<prev][<count][<size][<flag]{4..40}
//                           <---|---------+
//                           <-------------+T----------------->
//                  +--------|------------------------------------------------------------------------->
//                           <-------------------------------------|----------------+
//     prev = seek to last commit                              <---|-+
//     prev + trailer = size of commit                         <----T+--------------------------------->

// Block:  [hash>|<end]{10} ... [<size][<flag]{2..20}
//               +---------|------------------------>  end = seek to end of block
//                         <---|-+                     size = seek to end of header
//         <-------------------|-+10                   size + 10 = seek to start of block
//         <---------------------T+10--------------->  size + 10 + trailer = full block size

type Commit struct {
	flag  uint64 // flag values
	size  uint64 // size of the trailer
	count uint64 // number of entries
	prev  uint64 // previous commit

	tsize int
}

// Append marshals the trailer into binary form and appends it to data.
// It returns the new slice.
func (h *Commit) AppendTrailer(data []byte) []byte {
	h.flag |= TypePrevCommit
	if h.prev == 0 {
		h.flag &= TypeCommit
	}

	size := len(data)
	data = binary.AppendUvarint(data, h.size)
	data = binary.AppendUvarint(data, h.flag)
	data = binary.AppendUvarint(data, h.count)
	if h.prev != 0 {
		data = binary.AppendUvarint(data, h.prev)
	}
	reverse(data[size:])

	return data
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
// It reads a trailer from binary data, and sets the fields
// of the receiver to the values found in the header.
func (h *Commit) UnmarshalBinary(data []byte) error {
	if len(data) < minCommitSize {
		return fmt.Errorf("%w: bad data", ErrDecode)
	}

	var n int
	h.size, n = binary.Uvarint(data)
	data = data[n:]
	h.tsize += n

	h.flag, n = binary.Uvarint(data)
	data = data[n:]
	h.tsize += n

	h.count, n = binary.Uvarint(data)
	data = data[n:]
	h.tsize += n

	h.prev = h.size
	if h.flag&TypePrevCommit == TypePrevCommit {
		h.prev, n = binary.Uvarint(data)
		h.tsize += n
	}

	return nil
}

type Block struct {
	header

	size uint64
	flag uint64

	tsize int
}

func (h *Block) AppendHeader(data []byte) []byte {
	size := len(data)
	data = append(data, make([]byte, 10)...)
	copy(data, h.extra)
	if h.size == 0 {
		return data
	}
	hdata := binary.AppendUvarint(make([]byte, 0, 10), h.end)
	reverse(hdata)
	copy(data[size+10-len(hdata):], hdata)

	return data
}

// AppendTrailer marshals the footer into binary form and appends it to data.
// It returns the new slice.
func (h *Block) AppendTrailer(data []byte) []byte {
	size := len(data)

	h.flag |= TypeSegment
	data = binary.AppendUvarint(data, h.size)
	data = binary.AppendUvarint(data, h.flag)
	reverse(data[size:])

	return data
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
// It reads a footer from binary data, and sets the fields
// of the receiver to the values found in the footer.
func (h *Block) UnmarshalBinary(data []byte) error {
	if len(data) < minBlockSize {
		return fmt.Errorf("%w: bad data", ErrDecode)
	}

	var n int
	h.size, n = binary.Uvarint(data)
	data = data[n:]
	h.tsize += n

	h.flag, n = binary.Uvarint(data)
	data = data[n:]
	h.tsize += n

	copy(h.extra, data[:8])

	return nil
}

type logFile struct {
	header
	Commit
}

func (h *logFile) AppendMagic(data []byte) []byte {
	size := len(data)
	data = append(data, Magic[:]...)
	if h.end == 0 {
		return data
	}
	hdata := binary.AppendUvarint(make([]byte, 0, 10), h.end)
	reverse(hdata)
	copy(data[size+10-len(hdata):], hdata)

	return data
}
func (h *logFile) UnmarshalBinary(data []byte) error {
	return h.header.UnmarshalBinary(data)
}

// WriteLogFile writes a log file to w, given a list of segments.
// The caller is responsible for calling WriteAt on the correct offset.
// The function will return an error if any of the segments fail to write.
// The offset is the initial offset of the first segment, and will be
// incremented by the length of the segment on each write.
//
// The log file is written with the following format:
//   - A header with the magic, version, and flag (Dirty)
//   - A series of segments, each with:
//   - A footer with the length and hash of the segment
//   - The contents of the segment
//   - A header with the magic, version, flag (Clean), and end offset
func WriteLogFile(w io.WriterAt, segments ...io.Reader) error {
	_, err := w.WriteAt(Magic[:], 0)
	if err != nil {
		return err
	}

	lf := &LogWriter{
		WriterAt: w,
	}

	return lf.writeIter(w, iter.Seq[io.Reader](func(yield func(io.Reader) bool) {
		for _, s := range segments {
			if !yield(s) {
				return
			}
		}
	}))
}

type rw interface {
	io.ReaderAt
	io.WriterAt
}

func AppendLogFile(rw rw, segments ...io.Reader) error {
	logFile, err := ReadLogFile(rw)
	if err != nil {
		return err
	}
	lf := &LogWriter{
		WriterAt: rw,
		logFile:  logFile.logFile,
	}
	return lf.writeIter(rw, iter.Seq[io.Reader](func(yield func(io.Reader) bool) {
		for _, s := range segments {
			if !yield(s) {
				return
			}
		}
	}))

}

func WriteIter(w io.WriterAt, segments iter.Seq[io.Reader]) error {
	_, err := w.WriteAt(Magic[:], 0)
	if err != nil {
		return err
	}

	lf := &LogWriter{
		WriterAt: w,
	}

	return lf.writeIter(w, segments)
}

func (lf *LogWriter) writeIter(w io.WriterAt, segments iter.Seq[io.Reader]) error {
	for s := range segments {
		err := lf.writeSegment(s)
		if err != nil {
			return err
		}
		lf.count++
	}

	// Write the footer to the log file.
	// The footer is written at the current end of file position.
	n, err := lf.WriteAt(lf.AppendTrailer(make([]byte, 0, maxCommitSize)), int64(lf.end)+10)
	if err != nil {
		// If there is an error, return it.
		return err
	}
	lf.end += uint64(n)

	_, err = w.WriteAt(lf.AppendMagic(make([]byte, 0, 10)), 0)

	return err
}

type LogWriter struct {
	logFile
	io.WriterAt
}

// writeSegment writes a segment to the log file at the current end of file position.
// The segment is written in chunks of 1024 bytes, and the hash of the segment
func (lf *LogWriter) writeSegment(segment io.Reader) error {
	h := hash()
	head := Block{}
	start := int64(lf.end) + 10
	end := int64(lf.end) + 10

	// Write the header to the log file.
	// The footer is written at the current end of file position.
	n, err := lf.WriteAt(make([]byte, headerSize), start)
	if err != nil {
		// If there is an error, return it.
		return err
	}
	end += int64(n)
	lf.size += uint64(n)
	lf.end += uint64(n)

	// Write the segment to the log file.
	// The segment is written in chunks of 1024 bytes.
	for {
		// Read a chunk of the segment.
		buf := make([]byte, 1024)
		n, err := segment.Read(buf)
		if err != nil {
			// If the segment is empty, break the loop.
			if err == io.EOF {
				break
			}
			// If there is an error, return it.
			return err
		}

		// Compute the hash of the chunk.
		h.Write(buf[:n])

		// Write the chunk to the log file.
		// The chunk is written at the current end of file position.
		_, err = lf.WriteAt(buf[:n], end)
		if err != nil {
			// If there is an error, return it.
			return err
		}

		// Update the length of the segment.
		end += int64(n)
		head.size += uint64(n)
	}

	head.extra = h.Sum(nil)
	head.end += head.size

	// Write the footer to the log file.
	// The footer is written at the current end of file position.
	n, err = lf.WriteAt(head.AppendTrailer(make([]byte, 0, maxBlockSize)), end)
	if err != nil {
		// If there is an error, return it.
		return err
	}
	end += int64(n)
	head.end += uint64(n)

	// Update header to the log file.
	// The footer is written at the current end of file position.
	_, err = lf.WriteAt(head.AppendHeader(make([]byte, 0, headerSize)), start)
	if err != nil {
		// If there is an error, return it.
		return err
	}

	// Update the end of file position.
	lf.size += head.end
	lf.end += head.end
	return nil
}

// reverse reverses a slice in-place.
func reverse[T any](b []T) {
	l := len(b)
	for i := 0; i < l/2; i++ {
		b[i], b[l-i-1] = b[l-i-1], b[i]
	}
}

type LogReader struct {
	logFile
	io.ReaderAt
	Err error
}

// ReadLogFile reads a log file from the given io.ReaderAt. It returns a pointer to a LogFile, or an error if the file
// could not be read.
func ReadLogFile(reader io.ReaderAt) (*LogReader, error) {
	header := make([]byte, headerSize)
	n, err := rsr(reader, 0, 10).ReadAt(header, 0)
	if err != nil {
		return nil, err
	}
	header = header[:n]

	logFile := &LogReader{ReaderAt: reader}
	err = logFile.header.UnmarshalBinary(header)
	if err != nil {
		return nil, err
	}

	if logFile.end == 0 {
		return logFile, nil
	}

	commit := make([]byte, maxCommitSize)
	n, err = rsr(reader, 10, int64(logFile.end)).ReadAt(commit, 0)
	if n == 0 && err != nil {
		return nil, err
	}
	commit = commit[:n]

	err = logFile.Commit.UnmarshalBinary(commit)

	return logFile, err
}

// Iterate reads the log file and calls the given function for each segment.
// It passes an io.Reader that reads from the current segment. It will stop
// calling the function if the function returns false.
func (lf *LogReader) Iter() iter.Seq2[uint64, io.Reader] {
	var commits []*Commit
	for commit := range lf.iterCommits() {
		commits = append(commits, &commit)
	}
	if lf.Err != nil {
		return func(yield func(uint64, io.Reader) bool) {}
	}

	reverse(commits)

	return func(yield func(uint64, io.Reader) bool) {
		start := int64(10)
		for _, commit := range commits {
			size := int64(commit.prev)
			it := iterBlocks(io.NewSectionReader(lf, start, size), size)
			for i, block := range it {
				if !yield(i, block) {
					return
				}
			}

			start += size + int64(commit.tsize)
		}
	}
}

func iterBlocks(r io.ReaderAt, end int64) iter.Seq2[uint64, io.Reader] {
	var start int64
	var i uint64
	return func(yield func(uint64, io.Reader) bool) {
		for start < end {
			block := &Block{}
			buf := make([]byte, 10)
			n, err := rsr(r, int64(start), 10).ReadAt(buf, 0)
			if n == 0 && err != nil {
				return
			}
			start += int64(n)

			if err := block.header.UnmarshalBinary(buf); err != nil {
				return
			}

			buf = make([]byte, maxBlockSize)
			n, err = rsr(r, int64(start), int64(block.end)).ReadAt(buf, 0)
			if n == 0 && err != nil {
				return
			}
			buf = buf[:n]
			err = block.UnmarshalBinary(buf)
			if err != nil {
				return
			}

			if !yield(i, io.NewSectionReader(r, int64(start), int64(block.size))) {
				return
			}

			i++
			start += int64(block.end)
		}
	}
}


func (lf *LogReader) iterCommits() iter.Seq[Commit] {
	eof := lf.end + 10

	if eof <= 10 {
		return func(yield func(Commit) bool) {}
	}

	offset := eof - 10 - lf.prev - uint64(lf.tsize)
	return func(yield func(Commit) bool) {
		if !yield(lf.Commit) {
			return
		}

		for offset > 10 {
			commit := Commit{}
			buf := make([]byte, maxCommitSize)
			n, err := rsr(lf, 10, int64(offset)).ReadAt(buf, 0)
			if n == 0 && err != nil {
				lf.Err = err
				return
			}
			buf = buf[:n]
			err = commit.UnmarshalBinary(buf)
			if err != nil {
				lf.Err = err
				return
			}
			if !yield(commit) {
				return
			}
			offset -= commit.prev + uint64(commit.tsize)
		}
	}
}

// func (lf *LogReader) Rev() iter.Seq2[uint64, io.Reader] {
// 	end := lf.end + 10
// 	i := lf.count
// 	return func(yield func(uint64, io.Reader) bool) {

// 		for commit := range lf.iterCommits() {
// 			end -= uint64(commit.tsize)
// 			start := end - commit.prev - uint64(commit.tsize)
// 			for start > end{
// 				block := &Block{}
// 				buf := make([]byte, min(maxBlockSize, commit.size))
// 				n, err := lf.ReaderAt.ReadAt(buf, max(0, int64(end)-int64(len(buf))))
// 				if n == 0 && err != nil {
// 					lf.Err = err
// 					return
// 				}
// 				buf = buf[:n]
// 				err = block.UnmarshalBinary(buf)
// 				if err != nil {
// 					lf.Err = err
// 					return
// 				}
// 				if !yield(i, io.NewSectionReader(lf, int64(end-block.size)-int64(block.tsize), int64(block.size))) {
// 					return
// 				}
// 				end -= block.size + 10 + uint64(block.tsize)
// 				i--
// 			}

// 		}
// 	}
// }

func iterOne[I, T any](it iter.Seq2[I, T]) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, v := range it {
			if !yield(v) {
				return
			}
		}
	}
}

func rsr(r io.ReaderAt, offset, size int64) *revSegmentReader {
	r = io.NewSectionReader(r, offset, size)
	return &revSegmentReader{r, size}
}

type revSegmentReader struct {
	io.ReaderAt
	size int64
}

func (r *revSegmentReader) ReadAt(data []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, errors.New("negative offset")
	}

	if offset > int64(r.size) {
		return 0, io.EOF
	}

	o := r.size - int64(len(data)) - offset
	d := int64(len(data))
	if o < 0 {
		d = max(0, d+o)
	}

	i, err := r.ReaderAt.ReadAt(data[:d], max(0, o))
	reverse(data[:i])
	return i, err
}

// rdr    : 0 1 2|3 4 5 6|7 8 9 10
// rsr 6,4:       3 2 1 0
//                      6
//                ------- -4
//                0 1 2 3
// offset - size
// rdr    : 0 1 2|3 4 5 6|7 8 9 10
//                0 1 2 3
// offset=0      |-------|        d[:4], o=3  3-0=3
// offset=1     _|-----  |        d[:3], o=3  3-1=2
// offset=2   ___|---    |        d[:2], o=3  3-2=1
// offset=3 _____|-      |  	  d[:1], o=3  3-3=0
// offset=4+_____|       |  	  d[:0], o=3  3-4=0

// rdr    : 0 1 2|3 4 5 6|7 8 9 10
// offset=0      |-------|        d[:4], o=0 -> 3
// offset=0      |  -----|        d[:3], o=1 -> 4
// offset=0      |    ---|        d[:2], o=2 -> 5
// offset=0      |      -|        d[:1], o=3 -> 6
// offset=0      |       |        d[:0], o=4+-> 7

// rdr    : 0 1 2|3 4 5 6|7 8 9 10
// offset=4   ___|       |        d[:0], o=0
// offset=3     _|-      |        d[:1], o=0
// offset=2      |---    |        d[:2], o=0
// offset=1      |  ---  |        d[:2], o=1
// offset=0      |    ---|        d[:2], o=2
// offset=-1     |      -|_       d[:2], o=3
// offset=-2     |       |___     d[:2], o=4+

// o = max(0, offset - len)
// d =
