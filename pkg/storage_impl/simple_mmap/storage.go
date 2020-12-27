package simple_mmap

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/edsrzf/mmap-go"
	"github.com/paragor/parabase/pkg/engine"
)

// TODO
// TODO 1) set value: cant read meta: unexpected EOF - когда вышли за размер файла
// TODO

type Storage struct {
	file *os.File
	mmap mmap.MMap
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

func (s *Storage) Set(key, value []byte) error {
	offset := uint64(0)
	var innerErr error = nil
	hash := hashKey(key)
	err := s.iterate(func(curOffset uint64, node simpleNode) bool {
		if !node.meta.IsDeleted && node.meta.KeySize == uint64(len(key)) && hash == node.meta.KeyCRC32 && bytes.Equal(key, node.key) {
			innerErr = s.deleteByOffset(curOffset)
			if innerErr != nil {
				return true
			}
		}
		offset += node.meta.getNodeSize()
		return false
	}, false)
	if err != nil {
		return err
	}
	if innerErr != nil {
		return innerErr
	}
	node := simpleNode{}
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
func (s *Storage) iterate(iterator func(offset uint64, node simpleNode) bool, withDefence bool) error {
	offset := uint64(0)
	end := uint64(len(s.mmap))

	node := simpleNode{}
	reader := bytes.NewReader(nil)

	for offset < end {
		reader.Reset(s.mmap[offset:])
		if withDefence {
			err := node.Read(reader)
			if err != nil {
				return err
			}
			if !node.meta.isValid() {
				return nil
			}
		} else {
			err := node.ReadMeta(reader)
			if err != nil {
				return err
			}
			if !node.meta.isValid() {
				return nil
			}
			if end < offset+node.meta.getNodeSize() {
				return fmt.Errorf("read value too low (no def)")
			}
			node.key = s.mmap[offset+node.meta.getMetaSize() : offset+node.meta.getValueOffset()]
			node.value = s.mmap[offset+node.meta.getValueOffset() : offset+node.meta.getNodeSize()]
		}
		if iterator(offset, node) {
			return nil
		}

		offset += node.meta.getNodeSize()
	}

	return nil
}

func (s *Storage) Get(key []byte) ([]byte, error) {
	var value []byte
	found := false
	hash := hashKey(key)
	err := s.iterate(func(offset uint64, node simpleNode) bool {
		if !node.meta.IsDeleted && node.meta.KeySize == uint64(len(key)) && hash == node.meta.KeyCRC32 && bytes.Equal(key, node.key) {
			value = make([]byte, node.meta.ValueSize)
			copy(value, node.value)
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
	offset := uint64(0)
	found := false
	hash := hashKey(key)
	err := s.iterate(func(curOffset uint64, curNode simpleNode) bool {
		if !curNode.meta.IsDeleted && curNode.meta.KeySize == uint64(len(key)) && hash == curNode.meta.KeyCRC32 && bytes.Equal(key, curNode.key) {
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
	node := simpleNode{}
	err := node.ReadMeta(bytes.NewReader(s.mmap[offset:]))
	if err != nil {
		return err
	}
	if !node.meta.isValid() {
		return fmt.Errorf("invalid node for delete")
	}
	if node.meta.IsDeleted {
		return nil
	}
	node.Delete()
	buffer := bytes.NewBuffer(s.mmap[offset:])
	buffer.Reset()
	return node.WriteMeta(buffer)
}
