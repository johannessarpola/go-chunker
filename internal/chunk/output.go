package chunk

import (
	"fmt"
	"log"
	"os"
	"path"
	"sync"
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

type Worker struct {
	id   int
	file *os.File
}

func NewWorker(id int, output Output) (*Worker, error) {
	fname := fmt.Sprintf("%s_%d.%s", output.Prefix, id, output.Ext)
	fpath := path.Join(output.Dir, fname)
	fopen, err := os.Create(fpath)
	if err != nil {
		return nil, err
	}

	return &Worker{id: id, file: fopen}, nil
}

func (w *Worker) Write(v []byte) error {
	_, err := w.file.Write(append(v, '\n'))
	return err
}

func (w *Worker) Close() error {
	return w.file.Close()
}

// TODO Fix the typings at some point
func (np *ParWriter) Write(source Source[string], output Output) error {
	buffer := make(chan Message, np.workers)
	errors := make(chan error, np.workers)
	wg := &sync.WaitGroup{}
	wg.Add(3) // TODO Improve

	workers := make([]*Worker, np.workers)
	for i := 0; i < np.workers; i++ {
		w, err := NewWorker(i, output)
		workers[i] = w
		if err != nil {
			// TODO
			return err
		}
	}

	// TODO This can be parallized
	go func(source Source[string]) {
		for {
			v, i, ok := source.Next()
			if !ok {
				close(buffer)
				wg.Done()
				return
			}
			buffer <- Message{idx: i, msg: []byte(v)}
			i++
		}
	}(source)

	go func(buffer chan Message, size int) {
		defer close(errors)
		for v := range buffer {
			idx := v.idx
			dst := (idx / size) % np.workers
			err := workers[dst].Write(v.msg)
			if err != nil {
				errors <- err
			}
		}
		wg.Done()
	}(buffer, output.Size)

	go func() {
		for err := range errors {
			log.Println(err)
		}
		wg.Done()
	}()
	wg.Wait()
	// TODO Cleanup
	return nil
}
