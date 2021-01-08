package storage_engine_bench

import (
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/paragor/parabase/internal/tests"
	"github.com/paragor/parabase/pkg/engine"
)

func RandWriteAndReadBench(b *testing.B, storage engine.StorageEngine, keysCount int, option tests.BenchTimeTrackOption) {
	kvChecker := tests.NewKeyValueChecker()

	keysSuffix := make([]int, keysCount)
	for i := 0; i < keysCount; i++ {
		keysSuffix[i] = i
	}
	shuffle := func() {
		rand.Shuffle(keysCount, func(i, j int) {
			keysSuffix[i], keysSuffix[j] = keysSuffix[j], keysSuffix[i]
		})
	}
	b.ResetTimer()
	b.StartTimer()
	writeFunction := func(logResults bool) {
		start := time.Now()
		for _, suffix := range keysSuffix {
			key := []byte("key" + strconv.Itoa(suffix))
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
	readFunction := func(logResults bool) {
		start := time.Now()
		for _, suffix := range keysSuffix {
			key := []byte("key" + strconv.Itoa(suffix))
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
		shuffle()
		b.ResetTimer()
		b.StartTimer()
		readFunction(true)
		b.StopTimer()
	case tests.OnlyWriteOption:
		shuffle()
		b.ResetTimer()
		b.StartTimer()
		writeFunction(true)
		b.StopTimer()
	case tests.WriteAndReadOption:
		shuffle()
		b.ResetTimer()
		b.StartTimer()
		writeFunction(true)
		shuffle()
		readFunction(true)
		b.StopTimer()
	}
}
