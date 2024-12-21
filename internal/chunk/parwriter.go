package chunk

import (
	"fmt"
	"sync"
)

type ParWriter struct {
	workers   int
	printbars bool
}

func NewParWriter(workers int, printbars bool) *ParWriter {
	return &ParWriter{workers: workers, printbars: printbars}
}

func initializeChannels(workers int) []chan Message {
	channels := make([]chan Message, workers)
	for i := 0; i < workers; i++ {
		channels[i] = make(chan Message)
	}
	return channels
}

func initializeWorkers(workers int, output Output, chans []chan Message) ([]*WriteWorker, error) {
	writers := make([]*WriteWorker, workers)
	var err error
	for i := 0; i < workers; i++ {
		writers[i], err = NewWriteWorker(i, chans[i], output)
		if err != nil {
			return nil, fmt.Errorf("failed to create writer: %w", err)
		}
	}
	return writers, nil

}

// TODO Fix the typings at some point
func (np *ParWriter) Write(source Source[string], output Output) error {
	chans := initializeChannels(np.workers)

	writers, err := initializeWorkers(np.workers, output, chans)
	if err != nil {
		return fmt.Errorf("failed to create workers: %w", err)
	}

	arbitrer := NewArbitrer(source)
	// start writers
	wg := sync.WaitGroup{}
	wg.Add(len(writers))
	for _, worker := range writers {
		// run the writer
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

	arbitrer.Run(chans...)

	wg.Wait()
	return nil

}
