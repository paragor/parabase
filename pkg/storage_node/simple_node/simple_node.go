package simple_node

import (
	"encoding/binary"
	"fmt"
	"io"
)

type MetaInfo struct {
	IsDeleted bool
	KeySize   uint64
	ValueSize uint64

	Reserved1 uint64
	Reserved2 uint64
}

//var _metaInfoSize = uint64(unsafe.Sizeof(MetaInfo{}))
var _metaInfoSize = uint64(33)

func (mi *MetaInfo) GetMetaSize() uint64 {
	return _metaInfoSize
}
func (mi *MetaInfo) GetNodeSize() uint64 {
	return _metaInfoSize + mi.ValueSize + mi.KeySize
}
func (mi *MetaInfo) GetKeyOffset() uint64 {
	return _metaInfoSize
}
func (mi *MetaInfo) GetValueOffset() uint64 {
	return _metaInfoSize + mi.KeySize
}
func (mi *MetaInfo) IsValid() bool {
	return mi.KeySize > 0 && mi.ValueSize > 0
}

type SimpleNode struct {
	Meta  MetaInfo
	Key   []byte
	Value []byte
}

func (s *SimpleNode) SetKey(key []byte) {

	s.Key = make([]byte, len(key))
	copy(s.Key, key)
	s.Meta.KeySize = uint64(len(s.Key))
}
func (s *SimpleNode) SetValue(value []byte) {
	s.Value = make([]byte, len(value))
	copy(s.Value, value)
	s.Meta.ValueSize = uint64(len(s.Value))
}
func (s *SimpleNode) Delete() {
	s.Meta.IsDeleted = true
}

func (s *SimpleNode) Read(reader io.Reader) error {
	err := s.ReadMeta(reader)
	if err != nil {
		return fmt.Errorf("cant read Meta: %w", err)
	}
	s.Key = make([]byte, s.Meta.KeySize)
	n, err := reader.Read(s.Key)
	if uint64(n) < s.Meta.KeySize {
		return fmt.Errorf("read Key too low: %w", err)
	}

	s.Value = make([]byte, s.Meta.ValueSize)
	n, err = reader.Read(s.Value)
	if uint64(n) < s.Meta.ValueSize {
		return fmt.Errorf("read Value too low: %w", err)
	}

	return nil
}

func (s *SimpleNode) Write(writer io.Writer) error {

	s.Meta.KeySize = uint64(len(s.Key))
	s.Meta.ValueSize = uint64(len(s.Value))

	err := s.WriteMeta(writer)
	if err != nil {
		return fmt.Errorf("cant write Meta: %w", err)
	}
	n, err := writer.Write(s.Key)
	if err != nil {
		return fmt.Errorf("cant write Key: %w", err)
	}
	if uint64(n) < s.Meta.KeySize {
		return fmt.Errorf("write Key too low: %w", err)
	}

	n, err = writer.Write(s.Value)
	if err != nil {
		return fmt.Errorf("cant write Value: %w", err)
	}
	if uint64(n) < s.Meta.ValueSize {
		return fmt.Errorf("write Value too low: %w", err)
	}
	return nil
}

func (s *SimpleNode) ReadMeta(reader io.Reader) error {
	for _, value := range s.getMetaFieldsForSerialization() {
		err := binary.Read(reader, binary.BigEndian, value)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s *SimpleNode) getMetaFieldsForSerialization() []interface{} {
	return []interface{}{&s.Meta.KeySize, &s.Meta.ValueSize, &s.Meta.IsDeleted, &s.Meta.Reserved1, &s.Meta.Reserved2}
}
func (s *SimpleNode) WriteMeta(writer io.Writer) error {
	for _, value := range s.getMetaFieldsForSerialization() {
		err := binary.Write(writer, binary.BigEndian, value)
		if err != nil {
			return err
		}
	}
	return nil
}
