package storage_engine_bench

import (
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/paragor/parabase/internal/tests"
	"github.com/paragor/parabase/pkg/engine"
)

// ConcurrentWriteBench конкурентно пишет keysCount раз с goroutines горутин
func ConcurrentWriteBench(b *testing.B, storage engine.StorageEngine, keysCount int, goroutines int) {
	kvChecker := tests.NewKeyValueChecker()
	if keysCount < 1000 || goroutines < 2 {
		b.Fatal("$keysCount should be >= 1000, $goroutines should be >= 2")
		return
	}

	wg := sync.WaitGroup{}
	var groups [][]int
	prev := 0
	for i := 0; i < goroutines-1; i++ {
		group := int(math.Floor(float64(keysCount / goroutines)))
		groups = append(groups, []int{prev, prev + group})
		prev += group
	}
	// последствия округления
	groups = append(groups, []int{prev, prev + keysCount - groups[goroutines-2][1]})

	b.ResetTimer()
	start := time.Now()
	b.StartTimer()
	for _, interval := range groups {
		wg.Add(1)
		go (func(from, to int) {
			defer wg.Done()
			for i := from; i < to; i++ {
				key := []byte("key" + strconv.Itoa(i))
				err := storage.Set(key, kvChecker.GenValue(key))
				if err != nil {
					b.Error(err)
					return
				}
			}
		})(interval[0], interval[1])
	}
	wg.Wait()
	b.StopTimer()
	end := time.Now()
	secondsDur := end.Sub(start).Seconds()
	b.Logf("[CONCURRENT_WRITE] OP/s [%d] (%2f s)", int(float64(keysCount)/secondsDur), secondsDur)
	for i := 0; i < keysCount; i++ {
		key := []byte("key" + strconv.Itoa(i))
		value, err := storage.Get(key)
		if err != nil {
			b.Error(err, string(key))
			return
		}
		if !kvChecker.CheckValue(key, value) {
			b.Error(err, string(key))
			return
		}
	}
}
