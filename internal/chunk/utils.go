package chunk

import (
	"fmt"
	"os"
	"path/filepath"
)

func IsDir(path string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		return false, err
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return false, err
	}

	return stat.IsDir(), nil
}

func GetFirstExtensionInDir(dir string) (string, error) {
	de, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	for _, d := range de {
		ext := filepath.Ext(d.Name())
		if ext != "" {
			return ext, nil
		}
	}

	return "", fmt.Errorf("no file found in %s", dir)
}
