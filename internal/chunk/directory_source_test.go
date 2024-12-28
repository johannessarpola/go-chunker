package chunk

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewDirectorySource(t *testing.T) {
	td := path.Join("testdata", "folder")

	de, err := os.ReadDir(td)
	require.NoError(t, err)

	rs := int64(0)
	for _, f := range de {
		if f.IsDir() {
			continue
		}
		fp := path.Join(td, f.Name())
		result := <-asyncCountLines(fp)
		require.Nil(t, result.Err())

		rs += result.Value()
	}

	require.Greater(t, rs, int64(0))

	ds, err := NewDirectorySource(td)
	require.NoError(t, err)

	cnt := int64(0)
	for {
		_, _, ok := ds.Next()
		if !ok {
			break
		}
		cnt++
	}

	require.Equal(t, rs, cnt)

}
