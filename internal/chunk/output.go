package chunk

import (
	"fmt"
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

// TODO Fix the typings at some point
func (np *ParWriter) Write(source Source[string], output Output) error {
	var err error
	chans := make([]chan Message, np.workers)
	writers := make([]*WriteWorker, np.workers)

	for i := 0; i < np.workers; i++ {
		tx := make(chan Message, 1)
		chans[i] = tx
		writers[i], err = NewWriteWorker(i, tx, output)
		if err != nil {
			return fmt.Errorf("failed to create writer: %w", err)
		}
	}

	arbitrer := NewArbitrer(source)
	// start writers
	wg := sync.WaitGroup{}
	wg.Add(len(writers))
	for _, worker := range writers {
		go worker.Run(
			func(w *WriteWorker) {
				fmt.Printf("worker %d done, wrote to %s\n", w.id, w.file.Name())
				wg.Done()
			},
			func(w *WriteWorker, err error) {
				fmt.Printf("worker %d for %s failed: %e\n", w.id, w.file.Name(), err)
			},
		)
	}

	arbitrer.Run(output.Size, chans...)

	wg.Wait()
	return nil

}
