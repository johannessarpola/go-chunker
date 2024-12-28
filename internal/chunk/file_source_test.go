package chunk

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNewFileSource(t *testing.T) {
	td := path.Join("testdata", "data.txt")
	rs := <-asyncCountLines(td)

	require.NoError(t, rs.Err())

	s, err := NewFileSource(td)
	require.NoError(t, err)

	cnt := int64(0)
	for {
		_, _, ok := s.Next()
		if !ok {
			break
		}
		cnt++
	}

	require.Equal(t, rs.Value(), cnt)
}
