package storage_engine_bench

import (
	"strconv"
	"testing"
	"time"

	"github.com/paragor/parabase/internal/tests"
	"github.com/paragor/parabase/pkg/engine"
)

func SeqWriteAndReadBench(b *testing.B, storage engine.StorageEngine, keysCount int, option tests.BenchTimeTrackOption) {
	kvChecker := tests.NewKeyValueChecker()

	b.ResetTimer()
	b.StartTimer()
	writeFunction := func(logResults bool) {
		start := time.Now()
		for i := 0; i < keysCount; i++ {
			key := []byte("key" + strconv.Itoa(i))
			err := storage.Set(key, kvChecker.GenValue(key))
			if err != nil {
				b.Error(err)
				return
			}
		}
		end := time.Now()
		secondsDur := end.Sub(start).Seconds()
		if logResults {
			b.Logf("[WRTIE] OP/s [%d] (%2f s)", int(float64(keysCount)/secondsDur), secondsDur)
		}
	}
	readFunction := func (logResults bool) {
		start := time.Now()
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
		end := time.Now()
		secondsDur := end.Sub(start).Seconds()
		if logResults {
			b.Logf("[READ] OP/s [%d] (%2f s)", int(float64(keysCount)/secondsDur), secondsDur)
		}
	}
	switch option {
	case tests.OnlyReadOption:
		writeFunction(false)
		b.ResetTimer()
		b.StartTimer()
		readFunction(true)
		b.StopTimer()
	case tests.OnlyWriteOption:
		b.ResetTimer()
		b.StartTimer()
		writeFunction(true)
		b.StopTimer()
	case tests.WriteAndReadOption:
		b.ResetTimer()
		b.StartTimer()
		writeFunction(true)
		readFunction(true)
		b.StopTimer()
	}
}
