package in_memory

import (
	"testing"

	"github.com/paragor/parabase/internal/tests/storage_engine_tests"
)

func Test_WriteAndRead(t *testing.T) {
	storage,  err := NewStorage()
	if err != nil {
		t.Error(err)
		return
	}
	storage_engine_tests.WriteAndReadOperationsTest(t, storage)
}

func Test_Delete(t *testing.T) {
	storage,  err := NewStorage()
	if err != nil {
		t.Error(err)
		return
	}
	storage_engine_tests.DeleteOperationsTest(t, storage)
}

