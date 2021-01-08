package primitive_mmap

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/edsrzf/mmap-go"
	"github.com/paragor/parabase/pkg/engine"
	"github.com/paragor/parabase/pkg/storage_node/simple_node"
)

// TODO
// TODO 1) set value: cant read meta: unexpected EOF - когда вышли за размер файла
// TODO

type Storage struct {
	file *os.File
	mmap mmap.MMap
	m sync.RWMutex
}

func check(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func (s *Storage) Close() {
	check(s.mmap.Flush())
	check(s.mmap.Unmap())
	check(s.file.Close())
}

func NewStorage(filePath string) (*Storage, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("cant open database file: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("cant get stat: %w", err)
	}
	//mmap не умеет расширятся. Поэтому резервируем 5 мегабайт
	if stat.Size() < (5 << 20) {
		_, err = file.Write([]byte(strings.Repeat(string(rune(0x0)), 5<<20)))
		check(err)
	}

	mmapObj, err := mmap.Map(file, mmap.RDWR, 0)
	if err != nil {
		return nil, fmt.Errorf("cant mmap database file: %w", err)
	}
	return &Storage{file: file, mmap: mmapObj}, nil
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
	offset := uint64(0)
	var innerErr error = nil
	err := s.iterate(func(curOffset uint64, node simple_node.SimpleNode) bool {
		if !node.Meta.IsDeleted && node.Meta.KeySize == uint64(len(key)) && bytes.Equal(key, node.Key) {
			innerErr = s.deleteByOffset(curOffset)
			if innerErr != nil {
				return true
			}
		}
		offset += node.Meta.GetNodeSize()
		return false
	}, false)
	if err != nil {
		return err
	}
	if innerErr != nil {
		return innerErr
	}
	node := simple_node.SimpleNode{}
	node.SetKey(key)
	node.SetValue(value)
	buffer := bytes.NewBuffer(s.mmap[offset:])
	buffer.Reset()
	return node.Write(buffer)
}

// iterate
//          iterator - возвращает true когда нужно остановится
//          withDefence - true - копировать память в буфер (замедляет в 2 раза изза лишних аллокаций)
//                      - false - передавать key, value прям из mmap
func (s *Storage) iterate(iterator func(offset uint64, node simple_node.SimpleNode) bool, withDefence bool) error {
	offset := uint64(0)
	end := uint64(len(s.mmap))

	node := simple_node.SimpleNode{}
	reader := bytes.NewReader(nil)

	for offset < end {
		reader.Reset(s.mmap[offset:])
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
			node.Key = s.mmap[offset+node.Meta.GetMetaSize() : offset+node.Meta.GetValueOffset()]
			node.Value = s.mmap[offset+node.Meta.GetValueOffset() : offset+node.Meta.GetNodeSize()]
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
	var value []byte
	found := false
	err := s.iterate(func(offset uint64, node simple_node.SimpleNode) bool {
		if !node.Meta.IsDeleted && node.Meta.KeySize == uint64(len(key)) && bytes.Equal(key, node.Key) {
			value = make([]byte, node.Meta.ValueSize)
			copy(value, node.Value)
			found = true
			return true
		}
		return false
	}, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, engine.ErrorNotFound
	}
	return value, nil
}
func (s *Storage) Delete(key []byte) error {
	s.m.Lock()
	defer s.m.Unlock()
	offset := uint64(0)
	found := false
	err := s.iterate(func(curOffset uint64, curNode simple_node.SimpleNode) bool {
		if !curNode.Meta.IsDeleted && curNode.Meta.KeySize == uint64(len(key)) && bytes.Equal(key, curNode.Key) {
			offset = curOffset
			found = true
			return true
		}
		return false
	}, false)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	return s.deleteByOffset(offset)
}
func (s *Storage) deleteByOffset(offset uint64) error {
	node := simple_node.SimpleNode{}
	err := node.ReadMeta(bytes.NewReader(s.mmap[offset:]))
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
	buffer := bytes.NewBuffer(s.mmap[offset:])
	buffer.Reset()
	return node.WriteMeta(buffer)
}
