package simple_mmap

import (
	"strconv"
	"strings"
	"testing"

	"github.com/paragor/parabase/internal/tests/storage_engine_bench"
)

func Benchmark_SimpleMmap_Seq(b *testing.B) {
	for _, count := range []int{100, 1_000, 10_000} {
		b.Run(strconv.Itoa(count), func(b *testing.B) {
			storage, cleanRes, err := createSimpleMmapStorage(b, strings.ReplaceAll(b.Name(), "/", "_"))
			if err != nil {
				b.Error(err)
				return
			}
			defer cleanRes()
			storage_engine_bench.SeqWriteAndReadBench(b, storage, count)
		})
	}
}
