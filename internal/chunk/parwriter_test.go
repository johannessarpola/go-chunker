package chunk

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParWriterFile(t *testing.T) {
	td := path.Join("testdata", "data.txt")
	source, err := NewFileSource(td)
	require.NoError(t, err)

	_ = os.Mkdir("out", os.ModeDir)

	o := Output{
		Prefix: "data",
		Dir:    "out",
		Ext:    ".txt",
	}

	total, err := source.Total()
	require.NoError(t, err)

	workers := 10
	pw := NewParWriter(workers, total)

	err = pw.Run(source, o)
	require.NoError(t, err)
}

func TestParWriterDirectory(t *testing.T) {
	td := path.Join("testdata", "folder")
	source, err := NewDirectorySource(td)
	require.NoError(t, err)

	_ = os.Mkdir("out", os.ModeDir)

	o := Output{
		Prefix: "data",
		Dir:    "out",
		Ext:    ".txt",
	}

	total, err := source.Total()
	require.NoError(t, err)

	workers := 10
	pw := NewParWriter(workers, total)

	err = pw.Run(source, o)
	require.NoError(t, err)
}
