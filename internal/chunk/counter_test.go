package chunk

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAsyncCountLines(t *testing.T) {
	td := path.Join("testdata", "data.txt")
	rs := <-asyncCountLines(td)

	require.Equal(t, rs.Value(), int64(100))

	td = path.Join("testdata", "noexisting.txt")
	rs = <-asyncCountLines(td)

	require.Error(t, rs.Err())
}
