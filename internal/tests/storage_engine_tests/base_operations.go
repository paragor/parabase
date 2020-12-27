package storage_engine_tests

import (
	"testing"

	"github.com/paragor/parabase/internal/tests"
	"github.com/paragor/parabase/pkg/engine"
)

func WriteAndReadOperationsTest(t *testing.T, storage engine.StorageEngine) {
	kvChecker := tests.NewKeyValueChecker()

	keys := [][]byte{[]byte("key1"), []byte("key2")}
	for _, key := range keys {
		err := storage.Set(key, kvChecker.GenValue(key))
		if err != nil {
			t.Errorf("set value: %v (key=%s)", err, string(key))
			return
		}
	}
	for _, key := range keys {
		value, err := storage.Get(key)
		if err != nil {
			t.Errorf("get value: %v (key=%s)", err, string(key))
			return
		}
		if !kvChecker.CheckValue(key, value) {
			t.Errorf("value not expected key=%s value=%s", string(key), string(value))
			return
		}
	}
}

func DeleteOperationsTest(t *testing.T, storage engine.StorageEngine) {
	kvChecker := tests.NewKeyValueChecker()

	keys := [][]byte{[]byte("key1"), []byte("key2")}
	for _, key := range keys {
		err := storage.Set(key, kvChecker.GenValue(key))
		if err != nil {
			t.Errorf("set value: %v (key=%s)", err, string(key))
			return
		}
	}
	for _, key := range keys {
		value, err := storage.Get(key)
		if err != nil {
			t.Errorf("get value: %v (key=%s)", err, string(key))
			return
		}
		if !kvChecker.CheckValue(key, value) {
			t.Errorf("value not expected key=%s value=%s", string(key), string(value))
			return
		}

		err = storage.Delete(key)
		if err != nil {
			t.Errorf("delete value: %v (key=%s)", err, string(key))
			return
		}

		value, err = storage.Get(key)
		if err != engine.ErrorNotFound {
			t.Errorf("should error not found, get: %v (key=%s, value=%s)", err, string(key), string(value))
			return
		}
	}
}
