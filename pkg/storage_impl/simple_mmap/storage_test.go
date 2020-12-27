package simple_mmap

import (
	"os"
	"testing"

	"github.com/paragor/parabase/internal/tests"
	"github.com/paragor/parabase/internal/tests/storage_engine_tests"
	"github.com/paragor/parabase/pkg/engine"
)

func createSimpleMmapStorage(t testing.TB, name string) (storage engine.StorageEngine, cleanRes func(), err error) {
	databasePath, err := tests.GenerateCleanTmpFilePath(t, "simple_mmap"+name)
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
	storage, cleanRes, err := createSimpleMmapStorage(t, "write_and_read_test")
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanRes()
	storage_engine_tests.WriteAndReadOperationsTest(t, storage)
}

func Test_Delete(t *testing.T) {
	storage, cleanRes, err := createSimpleMmapStorage(t, "delet_test")
	if err != nil {
		t.Error(err)
		return
	}
	defer cleanRes()
	storage_engine_tests.DeleteOperationsTest(t, storage)
}
