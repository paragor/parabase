package with_hashtable_index_mmap

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/edsrzf/mmap-go"
	"github.com/paragor/parabase/pkg/engine"
	"github.com/paragor/parabase/pkg/storage_node/simple_node"
)

// TODO
// TODO 1) set value: cant read meta: unexpected EOF - когда вышли за размер файла
// TODO

type Storage struct {
	file      *os.File
	mmap      mmap.MMap
	index     map[string]uint64
	maxOffset uint64
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
	storage := &Storage{file: file, mmap: mmapObj, index: map[string]uint64{}}
	err = storage.updateIndex()
	return storage, err
}
func (s *Storage) updateIndex() error {
	return s.iterate(func(offset uint64, node simple_node.SimpleNode) bool {
		s.maxOffset = offset + node.Meta.GetNodeSize()
		s.index[string(node.Key)] = offset
		return false
	}, false)
}

func (s *Storage) Set(key, value []byte) error {
	err := s.Delete(key)
	if err != nil {
		return err
	}
	offset := s.maxOffset
	node := simple_node.SimpleNode{}
	node.SetKey(key)
	node.SetValue(value)
	buffer := bytes.NewBuffer(s.mmap[offset:])
	buffer.Reset()
	err = node.Write(buffer)
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
	offset, ok := s.index[string(key)]
	if !ok {
		return nil, engine.ErrorNotFound
	}

	node := simple_node.SimpleNode{}
	reader := bytes.NewReader(nil)

	reader.Reset(s.mmap[offset:])
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
	offset, ok := s.index[string(key)]
	if !ok {
		return nil
	}
	delete(s.index, string(key))
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