package primitive_mmap

import (
	"bytes"
	"os"
	"testing"

	"github.com/paragor/parabase/internal/tests"
	"github.com/paragor/parabase/internal/tests/storage_engine_tests"
	"github.com/paragor/parabase/pkg/engine"
)

func createPrimitiveMmapStorage(t testing.TB, name string) (storage engine.StorageEngine, cleanRes func(), err error) {
	databasePath, err := tests.GenerateCleanTmpFilePath(t, "primitive_mmap"+name)
	if err != nil {
		return nil, nil, err
	}
	storage, err = NewStorage(databasePath)
	cleanRes = func() {
		_ = os.Remove(databasePath)
	}
	return storage, cleanRes, err
}

func Test_WriteAndRead(t *testing.T) {
	storage, cleanRes, err := createPrimitiveMmapStorage(t, "write_and_read_test")
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanRes()
	storage_engine_tests.WriteAndReadOperationsTest(t, storage)
}

func Test_Delete(t *testing.T) {
	storage, cleanRes, err := createPrimitiveMmapStorage(t, "delete_test")
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanRes()
	storage_engine_tests.DeleteOperationsTest(t, storage)
}

func Test_Recreate(t *testing.T) {
	databasePath, err := tests.GenerateCleanTmpFilePath(t, "primitive_mmap_test_recreate")
	if err != nil {
		t.Error(err)
		return
	}
	storage, err := NewStorage(databasePath)
	if err != nil {
		t.Error(err)
		return
	}
	defer os.Remove(databasePath)

	keys := [][]byte{[]byte("key1"), []byte("key2")}
	value := []byte("one")
	for _, key := range keys {
		err := storage.Set(key, value)
		if err != nil {
			t.Errorf("set %v (key=%s)", err, string(key))
			return
		}
		getValue, err := storage.Get(key)
		if !bytes.Equal(getValue, value) {
			t.Errorf("get %v (key=%s) (%s != %s)", err, string(key), string(value), string(getValue))
			return
		}
	}

	anotherStorage, err := NewStorage(databasePath)
	if err != nil {
		t.Error(err)
		return
	}
	for _, key := range keys {
		getValue, err := anotherStorage.Get(key)
		if !bytes.Equal(getValue, value) {
			t.Errorf("anoteher storage should get %v (key=%s) (%s != %s)", err, string(key), string(value), string(getValue))
			return
		}
	}
}
