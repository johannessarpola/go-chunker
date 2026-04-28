package chunk

import (
	"fmt"
	"os"
	"testing"
)

func BenchmarkParWriter(b *testing.B) {
	// Create a temporary file for benchmarking
	tmpFile, err := os.CreateTemp("", "bench_data_*.txt")
	if err != nil {
		b.Fatal(err)
	}
	defer os.Remove(tmpFile.Name())

	// Populate it with 1 million lines
	for i := 0; i < 1000000; i++ {
		_, err := fmt.Fprintf(tmpFile, "line %d some data for benchmarking\n", i)
		if err != nil {
			b.Fatal(err)
		}
	}
	tmpFile.Close()

	_ = os.Mkdir("out_bench", os.ModePerm)
	defer os.RemoveAll("out_bench")

	o := Output{
		Prefix: "bench",
		Dir:    "out_bench",
		Ext:    ".txt",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		source, _ := NewFileSource(tmpFile.Name())
		total, _ := source.Total()
		workers := 10
		pw := NewParWriter(workers, total, false)
		_ = pw.Run(source, o)
	}
}
