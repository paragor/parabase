package simple_mmap

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"strings"
	"unsafe"

	"github.com/edsrzf/mmap-go"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/paragor/parabase/pkg/engine"
	"github.com/paragor/parabase/pkg/storage_impl/simple_mmap/fb"
)

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
	offset := uint32(0)
	for newOffset, obj, err := s.findFbObj(key, offset); err != engine.NotFoundError; {
		obj.MutateIsDeleted(true)
		offset = newOffset + uint32(obj.ValueLength()) + storageObjSize
	}
	builder := flatbuffers.NewBuilder(0)
	valPos := builder.CreateByteVector(value)
	keyPos := builder.CreateByteVector(key)

	fb.StorageObjStart(builder)
	fb.StorageObjAddKey(builder, keyPos)
	fb.StorageObjAddValue(builder, valPos)
	builder.Finish(fb.StorageObjEnd(builder))
	result := builder.FinishedBytes()

	offset += uint32(binary.PutUvarint(s.mmap[offset:], uint64(len(result))))

	copy(s.mmap[offset:], builder.FinishedBytes())

	return nil
}

func (s *Storage) Get(key []byte) ([]byte, error) {
	_, obj, err := s.findFbObj(key, 0)
	if err != nil {
		return nil, err
	}

	return obj.ValueBytes(), nil
}

var storageObjSize = uint32(unsafe.Sizeof(fb.StorageObj{}))

func (s *Storage) findFbObj(key []byte, startOffset uint32) (offset uint32, obj *fb.StorageObj, err error) {
	offset = startOffset

	for offset+storageObjSize < uint32(len(s.mmap)) {
		reader := bytes.NewReader(s.mmap[offset:])
		prev := reader.Len()
		metaOffset, err := binary.ReadUvarint(reader)
		if err != nil {
			return 0, nil, engine.NotFoundError
		}
		sizeOfUint := uint32(prev - reader.Len())
		obj = fb.GetRootAsStorageObj(s.mmap, flatbuffers.UOffsetT(offset+sizeOfUint))
		if metaOffset == 0 {
			return 0, nil, engine.NotFoundError
		}
		if obj.IsDeleted() {
			continue
		}

		if bytes.Compare(obj.KeyBytes(), key) == 0 {
			return offset, obj, nil
		}
		offset += uint32(metaOffset) + sizeOfUint
	}

	return 0, nil, engine.NotFoundError
}
func (s *Storage) calculateTotalSize(obj *fb.StorageObj) uint32 {
	return storageObjSize + uint32(obj.KeyLength()+obj.ValueLength())
}
