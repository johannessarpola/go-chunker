package chunk

import (
	"fmt"
	"os"
	"sync"
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

func (f *DirectorySource) ID() string {
	return f.dir
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
	total := atomic.Int64{}
	var err error
	cancel := make(chan struct{}, 1)
	wg := sync.WaitGroup{}
	for _, fs := range fd.fileSources {
		wg.Add(1)
		go func(fs *FileSource) {
			defer wg.Done()
			select {
			case <-cancel:
				return
			case r := <-fs.AsyncTotal():
				if r.Err() != nil {
					err = fmt.Errorf("error getting total for file source: %s", r.Err())
					cancel <- struct{}{}
					return
				}
				total.Add(r.Value())
			}
		}(fs)

	}

	wg.Wait()
	return total.Load(), err

}
