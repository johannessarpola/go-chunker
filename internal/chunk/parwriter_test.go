package chunk

import (
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParWriter_Write(t *testing.T) {
	td := path.Join("testdata", "data.txt")
	source, err := ReadFile(td)
	require.NoError(t, err)

	o := Output{
		Prefix: "data",
		Dir:    "out",
		Ext:    ".txt",
	}

	total, err := source.Total()
	require.NoError(t, err)

	workers := 4
	pw := NewParWriter(workers, total)

	err = pw.Run(source, o)
	require.NoError(t, err)
}
