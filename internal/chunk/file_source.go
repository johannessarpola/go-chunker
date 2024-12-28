package chunk

import (
	"bufio"
	"fmt"
	"os"
	"sync/atomic"
)

type FileSource struct {
	idx        atomic.Int64
	filePath   string
	fileReader <-chan string
}

func (f *FileSource) Next() (string, int64, bool) {
	line, ok := <-f.fileReader
	if !ok {
		return "", -1, false
	}
	return line, f.Inc(), true
}

func (f *FileSource) Total() (int64, error) {
	rs := <-asyncCountLines(f.filePath)
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

func NewFileSource(path string) (*FileSource, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	readChan := make(chan string, prereadBufferSz)
	// Preread the file into a channel
	go preread(file, readChan)

	return &FileSource{
		idx:        atomic.Int64{},
		filePath:   path,
		fileReader: readChan,
	}, nil
}
