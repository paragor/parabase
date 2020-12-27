package simple_mmap

import (
	"testing"

	"github.com/paragor/parabase/internal/tests/storage_engine_bench"
)

func Benchmark_SimpleMmap_Seq_RW__100(b *testing.B) {
	storage, cleanRes, err := createSimpleMmapStorage(b, "benchmark_write_and_read")
	if err != nil {
		b.Error(err)
		return
	}
	defer cleanRes()
	storage_engine_bench.SeqWriteAndReadBench(b, storage, 1_000)
}

func Benchmark_SimpleMmap_Seq_RW__1_000(b *testing.B) {
	storage, cleanRes, err := createSimpleMmapStorage(b, "benchmark_write_and_read")
	if err != nil {
		b.Error(err)
		return
	}
	defer cleanRes()
	storage_engine_bench.SeqWriteAndReadBench(b, storage, 1_000)
}

func Benchmark_SimpleMmap_Seq_RW__10_000(b *testing.B) {
	storage, cleanRes, err := createSimpleMmapStorage(b, "benchmark_write_and_read")
	if err != nil {
		b.Error(err)
		return
	}
	defer cleanRes()
	storage_engine_bench.SeqWriteAndReadBench(b, storage, 10_000)
}
