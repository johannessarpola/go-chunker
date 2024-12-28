package chunk

import (
	"bufio"
	"fmt"
	"os"
	"sync/atomic"
)

type Source[T any] interface {
	Next() (T, int64, bool)
	Total() (int64, error)
}

type Merger[T any] interface {
	Merge(src ...Source[T])
}

type FileSource struct {
	idx   atomic.Int64
	fpath string
	fread <-chan string
}

func (f *FileSource) Next() (string, int64, bool) {
	line, ok := <-f.fread
	if !ok {
		return "", -1, false
	}
	return line, f.Inc(), true
}

func (f *FileSource) Total() (int64, error) {
	rs := <-asyncCountLines(f.fpath)
	return rs.Get()
}

// Inc returns the index and then increments it.
func (f *FileSource) Inc() int64 {
	prev := f.idx.Load()
	f.idx.Add(1)
	return prev
}

func (f *FileSource) SetIndex(idx int64) {
	f.idx.Store(idx)
}

func (f *FileSource) Index() int64 {
	return f.idx.Load()
}

func ReadDir(folder string) (Source[string], error) {
	// TODO
	return nil, nil
}

func preread(f *os.File, out chan<- string) {
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		out <- scanner.Text()
	}
	close(out)
	_ = f.Close()
}

// TODO configurable buffer?
const prereadBufferSz = 64

func ReadFile(filename string) (Source[string], error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	fread := make(chan string, prereadBufferSz)
	// Preread the file into a channel
	go preread(file, fread)

	return &FileSource{
		fpath: filename,
		fread: fread,
	}, nil
}
