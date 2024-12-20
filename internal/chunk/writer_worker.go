package chunk

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"time"
)

const metaExt = "json"
const metaSuffix = "meta"

// WriteWorker writes messages from a `input` channel into a file.
type WriteWorker struct {
	id       int
	file     *os.File
	metaFile *os.File
	input    <-chan Message
}

// NewWriteWorker creates a new WriteWorker and determines the output file names.
func NewWriteWorker(id int, input <-chan Message, output Output) (*WriteWorker, error) {
	fname := fmt.Sprintf("%s_%d.%s", output.Prefix, id, output.Ext)
	mfname := fmt.Sprintf("%s_%d_%s.%s", output.Prefix, id, metaSuffix, metaExt)
	fopen, err := os.Create(path.Join(output.Dir, fname))
	if err != nil {
		return nil, err
	}

	fmopen, err := os.Create(path.Join(output.Dir, mfname))
	if err != nil {
		return nil, err
	}

	return &WriteWorker{id: id, file: fopen, input: input, metaFile: fmopen}, nil
}

// writerMeta is the metadata for the worker.
type writerMeta struct {
	ID       int    `json:"id"`
	File     string `json:"file"`
	Min      int    `json:"min"`
	Max      int    `json:"max"`
	Duration string `json:"duration"`
}

// Run runs the worker, writes the output file and companion metadata json file. `onDone` is called when done and `ònErr` if there was an error.
func (w *WriteWorker) Run(onDone func(w *WriteWorker), onErr func(w *WriteWorker, err error)) {
	defer func() {
		w.file.Close()
		onDone(w)
	}()
	start := time.Now()
	mx, mn := -1, -1
	for m := range w.input {
		if mn < 0 {
			mn = m.idx
		}
		if _, err := w.file.Write(append(m.msg, '\n')); err != nil {
			onErr(w, err)
			break
		}
		mx = m.idx
	}

	err := w.writeMeta(mn, mx, start)
	if err != nil {
		fmt.Errorf("error writing metadata; %e", err)
	}
}

func (w *WriteWorker) writeMeta(mn, mx int, start time.Time) error {
	defer w.metaFile.Close()
	enc := json.NewEncoder(w.metaFile)
	enc.SetIndent("", "    ")
	err := enc.Encode(&writerMeta{
		ID:       w.id,
		Max:      mx,
		Min:      mn,
		File:     w.file.Name(),
		Duration: time.Since(start).String(),
	})
	if err != nil {
		return err
	}
	return nil
}
