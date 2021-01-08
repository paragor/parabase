package in_memory

import (
	"runtime"
	"strconv"
	"testing"

	"github.com/paragor/parabase/internal/tests"
	"github.com/paragor/parabase/internal/tests/storage_engine_bench"
)

func Benchmark_InMemoryMmap_Seq(b *testing.B) {
	for _, count := range []int{100, 1_000, 10_000} {
		for _, option := range []tests.BenchTimeTrackOption{tests.OnlyWriteOption, tests.OnlyReadOption} {
			b.Run(string(option)+strconv.Itoa(count), func(b *testing.B) {
				storage, err := NewStorage()
				if err != nil {
					b.Error(err)
					return
				}
				storage_engine_bench.SeqWriteAndReadBench(b, storage, count, option)
			})
		}
	}
}
func Benchmark_InMemoryMmap_Rand(b *testing.B) {
	for _, count := range []int{100, 1_000, 10_000} {
		for _, option := range []tests.BenchTimeTrackOption{tests.OnlyWriteOption, tests.OnlyReadOption} {
			b.Run(string(option)+strconv.Itoa(count), func(b *testing.B) {
				storage, err := NewStorage()
				if err != nil {
					b.Error(err)
					return
				}
				storage_engine_bench.RandWriteAndReadBench(b, storage, count, option)
			})
		}
	}
}
func Benchmark_InMemoryMap_ConcurrentWrite(b *testing.B) {
	for _, count := range []int{1_000, 10_000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			storage, err := NewStorage()
			if err != nil {
				b.Error(err)
				return
			}
			storage_engine_bench.ConcurrentWriteBench(b, storage, count, runtime.NumCPU()*2)
		})
	}
}
