package chunk

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

type Source[T any] interface {
	Next() (T, bool)
}

type FileSource struct {
	mu      sync.Mutex
	file    *os.File
	scanner *bufio.Scanner
}

func (f *FileSource) Next() (string, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	hasToken := f.scanner.Scan()
	if !hasToken {
		_ = f.file.Close()
		f.scanner = nil
		return "", false
	}
	return f.scanner.Text(), true
}

func ReadFile(filename string) (Source[string], error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	scanner := bufio.NewScanner(file)

	return &FileSource{
		mu:      sync.Mutex{},
		file:    file,
		scanner: scanner,
	}, nil
}
