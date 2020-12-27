package storage_engine_bench

import (
	"strconv"
	"testing"

	"github.com/paragor/parabase/internal/tests"
	"github.com/paragor/parabase/pkg/engine"
)


func SeqWriteAndReadBench(b *testing.B, storage engine.StorageEngine, keysCount int) {
	kvChecker := tests.NewKeyValueChecker()

	b.ResetTimer()
	b.Run("write", func(b *testing.B) {
		b.StartTimer()
		for i := 0; i < keysCount; i++ {
			key := []byte("key" + strconv.Itoa(i))
			err := storage.Set(key, kvChecker.GenValue(key))
			if err != nil {
				b.Error(err)
				return
			}
		}
		b.StopTimer()
	})

	b.Run("read", func(b *testing.B) {
		b.StartTimer()
		for i := 0; i < keysCount; i++ {
			key := []byte("key" + strconv.Itoa(i))
			value, err := storage.Get(key)
			if err != nil {
				b.Error(err)
				return
			}
			if !kvChecker.CheckValue(key, value) {
				b.Error(err)
				return
			}
		}
		b.StopTimer()
	})
}

