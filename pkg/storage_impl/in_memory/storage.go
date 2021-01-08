package in_memory

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/paragor/parabase/pkg/engine"
	"github.com/paragor/parabase/pkg/storage_node/simple_node"
)

type Storage struct {
	data      []byte
	index     map[string]uint64
	maxOffset uint64
	m         sync.RWMutex
}

func (s *Storage) Close() {
}

func NewStorage() (*Storage, error) {
	data := make([]byte, 5<<20)
	return &Storage{data: data, index: map[string]uint64{}}, nil
}

func (s *Storage) extendStorageSize() error {
	oldData := s.data
	newData := make([]byte, len(oldData)*2)
	copy(newData, oldData)
	s.data = newData
	return nil
}

func (s *Storage) Iterate(iterator func(key, value []byte) bool) error {
	s.m.RLock()
	defer s.m.RUnlock()
	return s.iterate(func(offset uint64, node simple_node.SimpleNode) bool {
		return iterator(node.Key, node.Value)
	}, true)
}

func (s *Storage) Set(key, value []byte) error {
	s.m.Lock()
	defer s.m.Unlock()
	//delete
	if offset, ok := s.index[string(key)]; ok {
		delete(s.index, string(key))
		err := s.deleteByOffset(offset)
		if err != nil {
			return err
		}
	}
	offset := s.maxOffset
	node := simple_node.SimpleNode{}
	node.SetKey(key)
	node.SetValue(value)
	if len(s.data) < int(offset+node.Meta.GetNodeSize()) {
		err := s.extendStorageSize()
		if err != nil {
			return err
		}
	}
	buffer := bytes.NewBuffer(s.data[offset:])
	buffer.Reset()
	err := node.Write(buffer)
	if err == nil {
		s.index[string(key)] = offset
		s.maxOffset = offset + node.Meta.GetNodeSize()
	}
	return err
}

// iterate
//          iterator - возвращает true когда нужно остановится
//          withDefence - true - копировать память в буфер (замедляет в 2 раза изза лишних аллокаций)
//                      - false - передавать key, value прям из mmap
func (s *Storage) iterate(iterator func(offset uint64, node simple_node.SimpleNode) bool, withDefence bool) error {
	offset := uint64(0)
	end := uint64(len(s.data))

	node := simple_node.SimpleNode{}
	reader := bytes.NewReader(nil)

	for offset < end {
		reader.Reset(s.data[offset:])
		if withDefence {
			err := node.Read(reader)
			if err != nil {
				return err
			}
			if !node.Meta.IsValid() {
				return nil
			}
		} else {
			err := node.ReadMeta(reader)
			if err != nil {
				return err
			}
			if !node.Meta.IsValid() {
				return nil
			}
			if end < offset+node.Meta.GetNodeSize() {
				return fmt.Errorf("read value too low (no def)")
			}
			node.Key = s.data[offset+node.Meta.GetMetaSize() : offset+node.Meta.GetValueOffset()]
			node.Value = s.data[offset+node.Meta.GetValueOffset() : offset+node.Meta.GetNodeSize()]
		}
		if iterator(offset, node) {
			return nil
		}

		offset += node.Meta.GetNodeSize()
	}

	return nil
}

func (s *Storage) Get(key []byte) ([]byte, error) {
	s.m.RLock()
	defer s.m.RUnlock()
	offset, ok := s.index[string(key)]
	if !ok {
		return nil, engine.ErrorNotFound
	}

	node := simple_node.SimpleNode{}
	reader := bytes.NewReader(nil)

	reader.Reset(s.data[offset:])
	err := node.Read(reader)
	if err != nil {
		return nil, err
	}
	if !node.Meta.IsValid() {
		return nil, nil
	}
	return node.Value, nil
}
func (s *Storage) Delete(key []byte) error {
	s.m.Lock()
	defer s.m.Unlock()
	offset, ok := s.index[string(key)]
	if !ok {
		return nil
	}
	delete(s.index, string(key))
	return s.deleteByOffset(offset)
}
func (s *Storage) deleteByOffset(offset uint64) error {
	node := simple_node.SimpleNode{}
	err := node.ReadMeta(bytes.NewReader(s.data[offset:]))
	if err != nil {
		return err
	}
	if !node.Meta.IsValid() {
		return fmt.Errorf("invalid node for delete")
	}
	if node.Meta.IsDeleted {
		return nil
	}
	node.Delete()
	buffer := bytes.NewBuffer(s.data[offset:])
	buffer.Reset()
	return node.WriteMeta(buffer)
}
