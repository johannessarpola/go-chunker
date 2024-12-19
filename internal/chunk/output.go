package chunk

import (
	"fmt"
	"log"
	"os"
	"path"
	"time"
)

type ParWriter struct {
	workers int
}

func NewParWriter(workers int) *ParWriter {
	return &ParWriter{workers: workers}
}

type Output struct {
	Prefix string
	Dir    string
	Ext    string
	Size   int
}

func NewOutput(prefix, dir, ext string) Output {
	return Output{Prefix: prefix, Dir: dir, Ext: ext}
}

type Message struct {
	idx int
	msg []byte
}

type WriteWorker struct {
	id   int
	file *os.File
}

func NewWorker(id int, output Output) (*WriteWorker, error) {
	fname := fmt.Sprintf("%s_%d.%s", output.Prefix, id, output.Ext)
	fpath := path.Join(output.Dir, fname)
	fopen, err := os.Create(fpath)
	if err != nil {
		return nil, err
	}

	return &WriteWorker{id: id, file: fopen}, nil
}

func (w *WriteWorker) Write(v []byte) error {
	_, err := w.file.Write(append(v, '\n'))
	return err
}

func (w *WriteWorker) Close() error {
	return w.file.Close()
}

type ReadWorker struct {
	source Source[string]
	done   bool
	out    chan<- Message
}

func NewReadWorker(source Source[string], out chan<- Message) *ReadWorker {
	return &ReadWorker{source: source, done: false, out: out}
}

func (w *ReadWorker) Run() {
	for {
		v, idx, ok := w.source.Next()
		if !ok {
			w.done = true
			close(w.out)
			return
		}
		w.out <- Message{idx: idx, msg: []byte(v)}
	}
}

// TODO Fix the typings at some point
func (np *ParWriter) Write(source Source[string], output Output) error {
	buffer := make(chan Message, np.workers)
	errors := make(chan error, np.workers)

	chans := make([]chan Message, np.workers)
	for i := 0; i < np.workers; i++ {
		chans = append(chans, make(chan Message, 1))
	}

	rworkers := make([]*ReadWorker, np.workers)
	for _, c := range chans {
		w := NewReadWorker(source, c)
		rworkers = append(rworkers, w)
	}

	allDone := make(chan struct{})
	go func ()  {
		for {
			for _, rw := range rworkers {
				if !rw.done {
					return
				}
			}
		}

		allDone<- struct{}{}
		close(allDone)
	}

	wworkers := make([]*WriteWorker, np.workers)
	for i := 0; i < np.workers; i++ {
		w, err := NewWorker(i, output)
		wworkers[i] = w
		if err != nil {
			// TODO
			return err
		}
	}

	for _, rworker := range rworkers {
		go rworker.Produce(buffer)
	}

	go func(buffer chan Message, size int) {
		defer close(errors)
		wwg.Add(1)
		for v := range buffer {
			idx := v.idx
			dst := (idx / size) % np.workers
			err := wworkers[dst].Write(v.msg)
			if err != nil {
				errors <- err
			}
		}
		wwg.Done()
	}(buffer, output.Size)

	go func() {
		for err := range errors {
			log.Println(err)
		}
	}()

	rwg.Wait()
	return nil
}
