package chunk

import (
	"bufio"
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
	writer   *bufio.Writer
	metaFile *os.File
	input    <-chan Message
}

// NewWriteWorker creates a new WriteWorker and determines the output file names.
func NewWriteWorker(id int, input <-chan Message, output Output) (*WriteWorker, error) {
	fname := fmt.Sprintf("%s_%d%s", output.Prefix, id, output.Ext)
	mfname := fmt.Sprintf("%s_%d_%s.%s", output.Prefix, id, metaSuffix, metaExt)
	fopen, err := os.Create(path.Join(output.Dir, fname))
	if err != nil {
		return nil, err
	}

	fmopen, err := os.Create(path.Join(output.Dir, mfname))
	if err != nil {
		return nil, err
	}

	buf := bufio.NewWriter(fopen)
	return &WriteWorker{id: id, file: fopen, writer: buf, input: input, metaFile: fmopen}, nil
}

// writerMeta is the metadata for the worker.
type writerMeta struct {
	ID             int    `json:"id"`
	File           string `json:"file"`
	Min            int64  `json:"min"`
	Max            int64  `json:"max"`
	AliveDuration  string `json:"alive_duration"`
	ActiveDuration string `json:"active_duration"`
}

// Run runs the worker, writes the output file and companion metadata json file. `onDone` is called when done and `ònErr` if there was an error.
func (w *WriteWorker) Run(onHandled func(m *Message), onComplete func(w *WriteWorker, err error)) {
	defer func() {
		_ = w.writer.Flush()
		_ = w.file.Close()
		onComplete(w, nil)
	}()
	start := time.Now()
	active := time.Time{}
	mx, mn := int64(-1), int64(-1)
	for m := range w.input {

		if active == (time.Time{}) { // first message or after last one?
			active = time.Now()
		}

		if mn < 0 {
			mn = m.idx
		}
		if _, err := w.writer.Write(append(m.msg, '\n')); err != nil {
			break
		}
		onHandled(&m)
		mx = m.idx
	}

	end := time.Now()
	err := w.writeMeta(mn, mx, start, active, end)
	if err != nil {
		fmt.Printf("error writing metadata; %e", err)
	}
}

func (w *WriteWorker) writeMeta(mn, mx int64, start, activeStart, end time.Time) error {
	defer w.metaFile.Close()

	enc := json.NewEncoder(w.metaFile)
	enc.SetIndent("", "    ")
	err := enc.Encode(&writerMeta{
		ID:             w.id,
		Max:            mx,
		Min:            mn,
		File:           w.file.Name(),
		AliveDuration:  time.Since(start).String(),
		ActiveDuration: end.Sub(activeStart).String(),
	})
	if err != nil {
		return err
	}
	return nil
}
