package chunk

import (
	"fmt"
	"os"
	"sync/atomic"
)

type DirectorySource struct {
	dir         string
	fileSources []*FileSource
	idx         atomic.Int64
}

func NewDirectorySource(dir string) (*DirectorySource, error) {

	de, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var sources []*FileSource
	for _, entry := range de {
		if entry.IsDir() { // skip directories
			continue
		}
		filePath := dir + "/" + entry.Name()
		fs, err := NewFileSource(filePath) // create FileSource for each file in directory
		if err != nil {
			return nil, err
		}
		sources = append(sources, fs)
	}

	return &DirectorySource{dir: dir, fileSources: sources}, nil
}

// Inc returns the index and then increments it.
func (f *DirectorySource) Inc() int64 {
	prev := f.idx.Load()
	f.idx.Add(1)
	return prev
}

func (fd *DirectorySource) Next() (string, int64, bool) {
	if len(fd.fileSources) == 0 {
		return "", -1, false
	}
	// we ignore your index and substitute it with our own.
	d, _, ok := fd.fileSources[0].Next()
	if !ok {
		fd.fileSources = fd.fileSources[1:]
		return fd.Next()
	}

	return d, fd.Inc(), ok
}

func (fd *DirectorySource) Total() (int64, error) {
	total := int64(0)
	for _, fs := range fd.fileSources {
		subtotal, err := fs.Total()
		if err != nil {
			return -1, fmt.Errorf("error getting total for file source: %s", err)
		}
		total += subtotal
	}

	return total, nil

}
