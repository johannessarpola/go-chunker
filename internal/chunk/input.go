package chunk

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

type Source[T any] interface {
	Next() (T, int, bool)
	Total() (int64, error)
}

type FileSource struct {
	mu      sync.Mutex
	idx     int
	fpath   string
	file    *os.File
	scanner *bufio.Scanner
}

func (f *FileSource) Next() (string, int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	hasToken := f.scanner.Scan()
	if !hasToken {
		_ = f.file.Close()
		return "", -1, false
	}
	idx := f.idx
	f.idx++
	return f.scanner.Text(), idx, true
}

func (f *FileSource) Total() (int64, error) {
	rs := <-asyncCountLines(f.fpath)
	return rs.Get()
}

func ReadFile(filename string) (Source[string], error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	scanner := bufio.NewScanner(file)

	return &FileSource{
		mu:      sync.Mutex{},
		fpath:   filename,
		file:    file,
		scanner: scanner,
	}, nil
}
