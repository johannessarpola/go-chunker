package chunk

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

const metaExt = "json"
const metaSuffix = "meta"

type WriteWorker struct {
	id       int
	file     *os.File
	metaFile *os.File
	input    <-chan Message
}

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

type Meta struct {
	ID   int    `json:"id"`
	File string `json:"file"`
	Min  int    `json:"min"`
	Max  int    `json:"max"`
}

func (w *WriteWorker) Run(onDone func(w *WriteWorker), onErr func(w *WriteWorker, err error)) {
	defer func() {
		w.file.Close()
		onDone(w)
	}()
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

	meta := Meta{
		ID:   w.id,
		Max:  mx,
		Min:  mn,
		File: w.file.Name(),
	}
	enc := json.NewEncoder(w.metaFile)
	enc.SetIndent("", "    ")
	err := enc.Encode(meta)
	if err != nil {
		fmt.Println(err)
	}
	w.metaFile.Close()
}
