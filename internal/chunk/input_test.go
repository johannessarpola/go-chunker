package chunk

import (
	"bufio"
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"testing"
)

func countLines(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineCount := 0

	for scanner.Scan() {
		lineCount++
	}

	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("error reading file: %w", err)
	}

	return lineCount, nil
}

func TestReadFile(t *testing.T) {

	td := path.Join("testdata", "data.txt")
	lc, err := countLines(td)
	require.NoError(t, err)

	s, err := ReadFile(td)
	require.NoError(t, err)

	cnt := 0
	for {
		v, ok := s.Next()
		fmt.Println(v)
		if !ok {
			break
		}
		cnt++
	}

	require.Equal(t, lc, cnt)
}
