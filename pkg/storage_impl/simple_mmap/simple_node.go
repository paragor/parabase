package simple_mmap

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
)

type metaInfo struct {
	IsDeleted bool
	KeySize   uint64
	ValueSize uint64
	KeyCRC32  uint64

	Reserved2 uint64
}

var crc32Hasher = crc32.NewIEEE()

func hashKey(key []byte) uint64 {
	defer crc32Hasher.Reset()
	crc32Hasher.Write(key)
	return uint64(crc32Hasher.Sum32())
}

//var _metaInfoSize = uint64(unsafe.Sizeof(metaInfo{}))
var _metaInfoSize = uint64(33)

func (mi *metaInfo) getMetaSize() uint64 {
	return _metaInfoSize
}
func (mi *metaInfo) getNodeSize() uint64 {
	return _metaInfoSize + mi.ValueSize + mi.KeySize
}
func (mi *metaInfo) getKeyOffset() uint64 {
	return _metaInfoSize
}
func (mi *metaInfo) getValueOffset() uint64 {
	return _metaInfoSize + mi.KeySize
}
func (mi *metaInfo) isValid() bool {
	return mi.KeySize > 0 && mi.ValueSize > 0
}

type simpleNode struct {
	meta  metaInfo
	key   []byte
	value []byte
}

func (s *simpleNode) SetKey(key []byte) {

	s.key = make([]byte, len(key))
	s.meta.KeyCRC32 = hashKey(key)
	copy(s.key, key)
	s.meta.KeySize = uint64(len(s.key))
}
func (s *simpleNode) SetValue(value []byte) {
	s.value = make([]byte, len(value))
	copy(s.value, value)
	s.meta.ValueSize = uint64(len(s.value))
}
func (s *simpleNode) Delete() {
	s.meta.IsDeleted = true
}

func (s *simpleNode) Read(reader io.Reader) error {
	err := s.ReadMeta(reader)
	if err != nil {
		return fmt.Errorf("cant read meta: %w", err)
	}
	s.key = make([]byte, s.meta.KeySize)
	n, err := reader.Read(s.key)
	if uint64(n) < s.meta.KeySize {
		return fmt.Errorf("read key too low: %w", err)
	}

	s.value = make([]byte, s.meta.ValueSize)
	n, err = reader.Read(s.value)
	if uint64(n) < s.meta.ValueSize {
		return fmt.Errorf("read value too low: %w", err)
	}

	return nil
}

func (s *simpleNode) Write(writer io.Writer) error {

	s.meta.KeySize = uint64(len(s.key))
	s.meta.ValueSize = uint64(len(s.value))

	err := s.WriteMeta(writer)
	if err != nil {
		return fmt.Errorf("cant write meta: %w", err)
	}
	n, err := writer.Write(s.key)
	if err != nil {
		return fmt.Errorf("cant write key: %w", err)
	}
	if uint64(n) < s.meta.KeySize {
		return fmt.Errorf("write key too low: %w", err)
	}

	n, err = writer.Write(s.value)
	if err != nil {
		return fmt.Errorf("cant write value: %w", err)
	}
	if uint64(n) < s.meta.ValueSize {
		return fmt.Errorf("write value too low: %w", err)
	}
	return nil
}

func (s *simpleNode) ReadMeta(reader io.Reader) error {
	for _, value := range []interface{}{&s.meta.KeySize, &s.meta.ValueSize, &s.meta.IsDeleted, &s.meta.KeyCRC32, &s.meta.Reserved2} {
		err := binary.Read(reader, binary.BigEndian, value)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *simpleNode) WriteMeta(writer io.Writer) error {
	for _, value := range []interface{}{&s.meta.KeySize, &s.meta.ValueSize, &s.meta.IsDeleted, &s.meta.KeyCRC32, &s.meta.Reserved2} {
		err := binary.Write(writer, binary.BigEndian, value)
		if err != nil {
			return err
		}
	}
	return nil
}
