package chunk

import (
	"fmt"
	"os"
)

type DirectorySource struct {
	dir         string
	fileSources []Source[string]
}

func NewDirectorySource(dir string) (*DirectorySource, error) {

	de, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var sources []Source[string]
	for _, entry := range de {
		//
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

func (fd *DirectorySource) Next() (string, int64, bool) {

	if len(fd.fileSources) > 0 {
		for _, f := range fd.fileSources {
			if s, l, ok := f.Next(); ok {
				return s, l, true
			} else {
				fd.fileSources = fd.fileSources[1:]
			}
		}
	}
	return "", -1, false
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
