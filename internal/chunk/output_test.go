package chunk

import (
	"github.com/stretchr/testify/require"
	"path"
	"testing"
)

func TestParWriter_Write(t *testing.T) {
	td := path.Join("testdata", "data.txt")
	source, err := ReadFile(td)
	require.NoError(t, err)

	o := Output{
		Prefix: "data",
		Dir:    "out",
		Ext:    "txt",
		Size:   10,
	}

	workers := 10
	pw := NewParWriter(workers)

	err = pw.Write(source, o)
	require.NoError(t, err)
}
