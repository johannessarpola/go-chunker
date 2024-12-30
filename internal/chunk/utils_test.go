package chunk

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsDir(t *testing.T) {
	p := "testdata"
	b, err := IsDir(p)

	require.NoError(t, err)
	require.True(t, b)

	p = path.Join("testdata", "data.txt")
	b, err = IsDir(p)

	require.NoError(t, err)
	require.False(t, b)
}
