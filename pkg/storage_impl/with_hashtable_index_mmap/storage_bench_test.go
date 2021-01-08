package with_hashtable_index_mmap

import (
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/paragor/parabase/internal/tests"
	"github.com/paragor/parabase/internal/tests/storage_engine_bench"
)

func Benchmark_WithHashTableIndexMmap_Seq(b *testing.B) {
	for _, count := range []int{100, 1_000, 10_000} {
		for _, option := range []tests.BenchTimeTrackOption{tests.OnlyWriteOption, tests.OnlyReadOption} {
			b.Run(string(option)+strconv.Itoa(count), func(b *testing.B) {
				storage, cleanRes, err := createWithHashTableIndexMmapStorage(b, strings.ReplaceAll(b.Name(), "/", "_"))
				if err != nil {
					b.Error(err)
					return
				}
				defer cleanRes()
				storage_engine_bench.SeqWriteAndReadBench(b, storage, count, option)
			})
		}
	}
}

func Benchmark_WithHashTableIndexMmap_Rand(b *testing.B) {
	for _, count := range []int{100, 1_000, 10_000} {
		for _, option := range []tests.BenchTimeTrackOption{tests.OnlyWriteOption, tests.OnlyReadOption} {
			b.Run(string(option)+strconv.Itoa(count), func(b *testing.B) {
				storage, cleanRes, err := createWithHashTableIndexMmapStorage(b, strings.ReplaceAll(b.Name(), "/", "_"))
				if err != nil {
					b.Error(err)
					return
				}
				defer cleanRes()
				storage_engine_bench.RandWriteAndReadBench(b, storage, count, option)
			})
		}
	}
}

func Benchmark_WithHashTableIndexMmap_ConcurrentWrite(b *testing.B) {
	for _, count := range []int{1_000, 10_000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			storage, cleanRes, err := createWithHashTableIndexMmapStorage(b, strings.ReplaceAll(b.Name(), "/", "_"))
			if err != nil {
				b.Error(err)
				return
			}
			defer cleanRes()
			storage_engine_bench.ConcurrentWriteBench(b, storage, count, runtime.NumCPU()*10)
		})
	}
}
