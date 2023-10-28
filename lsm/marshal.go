package lsm

import (
	"bytes"
	"encoding"
	"encoding/binary"
	"fmt"
)

type entry struct {
	key   string
	value uint64
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (e *entry) MarshalBinary() (data []byte, err error) {
	data = make([]byte, len(e.key), len(e.key)+binary.MaxVarintLen16)
	copy(data, e.key)

	data = binary.AppendUvarint(data, e.value)
	reverse(data[len(e.key):])
	return data, err
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (e *entry) UnmarshalBinary(data []byte) error {
	// fmt.Println("unmarshal", data, string(data))

	if len(data) < binary.MaxVarintLen16 {
		return fmt.Errorf("%w: bad data", ErrDecode)
	}
	head := make([]byte, binary.MaxVarintLen16)
	copy(head, data[max(0, len(data)-cap(head)):])
	reverse(head)

	size := 0
	e.value, size = binary.Uvarint(head)
	if size == 0 {
		return fmt.Errorf("%w: invalid data", ErrDecode)
	}
	e.key = string(data[:len(data)-size])

	return nil
}

var _ encoding.BinaryMarshaler = (*entry)(nil)
var _ encoding.BinaryUnmarshaler = (*entry)(nil)

type entries []entry

// MarshalBinary implements encoding.BinaryMarshaler.
func (lis *entries) MarshalBinary() (data []byte, err error) {
	var buf bytes.Buffer

	for _, e := range *lis {
		d, err := e.MarshalBinary()
		if err != nil {
			return nil, err
		}

		_, err = buf.Write(d)
		if err != nil {
			return nil, err
		}

		_, err = buf.Write(reverse(binary.AppendUvarint(make([]byte, 0, binary.MaxVarintLen32), uint64(len(d)))))
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), err
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (lis *entries) UnmarshalBinary(data []byte) error {
	head := make([]byte, binary.MaxVarintLen16)
	pos := uint64(len(data))

	for pos > 0 {
		copy(head, data[max(0, pos-uint64(cap(head))):])
		length, size := binary.Uvarint(reverse(head))

		e := entry{}
		if err := e.UnmarshalBinary(data[max(0, pos-(length+uint64(size))) : pos-uint64(size)]); err != nil {
			return err
		}
		*lis = append(*lis, e)

		pos -= length + uint64(size)
	}
	reverse(*lis)
	return nil
}

var _ encoding.BinaryMarshaler = (*entries)(nil)
var _ encoding.BinaryUnmarshaler = (*entries)(nil)

type segment struct {
	entries entries
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (s *segment) MarshalBinary() (data []byte, err error) {
	head := header{
		entries: uint64(len(s.entries)),
	}

	data, err = s.entries.MarshalBinary()
	if err != nil {
		return nil, err
	}

	head.datalen = uint64(len(data))

	h := hash()
	h.Write(data)
	head.sig = h.Sum(nil)

	return head.Append(data), err
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (s *segment) UnmarshalBinary(data []byte) error {
	head, err := ReadHead(data)
	if err != nil {
		return err
	}

	h := hash()
	h.Write(data[:head.datalen])
	if !bytes.Equal(head.sig, h.Sum(nil)) {
		return fmt.Errorf("%w: invalid checksum", ErrDecode)
	}

	s.entries = make(entries, 0, head.entries)
	return s.entries.UnmarshalBinary(data[:head.datalen])
}
