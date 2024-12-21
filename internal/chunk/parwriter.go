package chunk

import (
	"fmt"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

type ParWriter struct {
	workers int
}

func NewParWriter(workers int) *ParWriter {
	return &ParWriter{workers: workers}
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
func (np *ParWriter) Run(source Source[string], output Output) error {
	chans := initializeChannels(np.workers)

	writers, err := initializeWorkers(np.workers, output, chans)
	if err != nil {
		return fmt.Errorf("failed to create workers: %w", err)
	}

	arbitrer := NewArbitrer(source)

	// start writers
	wg := sync.WaitGroup{}
	wg.Add(len(writers))

	// progress bar
	p := mpb.New(mpb.WithWaitGroup(&wg))
	total, err := source.Total()
	if err != nil {
		return err
	}
	workerTotal := total / int64(len(writers))

	for _, worker := range writers {
		name := fmt.Sprintf("Bar#%d:", worker.id)
		bar := p.AddBar(int64(workerTotal),
			mpb.PrependDecorators(
				// simple name decorator
				decor.Name(name),
				// decor.DSyncWidth bit enables column width synchronization
				decor.Percentage(decor.WCSyncSpace),
			),
			mpb.AppendDecorators(
				// replace ETA decorator with "done" message, OnComplete event
				decor.OnComplete(
					// ETA decorator with ewma age of 30
					decor.EwmaETA(decor.ET_STYLE_GO, 30, decor.WCSyncWidth), "done",
				),
			),
		)

		start := time.Now()
		// run the writer
		go worker.Run(
			func(m *Message) {
				bar.EwmaIncrement(time.Since(start))
				time.Sleep(10 * time.Millisecond)
			},
			func(w *WriteWorker, err error) {
				//fmt.Printf("worker %d done, wrote to %s\n", w.id, w.file.Name())
				wg.Done()
			},
		)
	}

	arbitrer.Run(chans...)

	p.Wait()
	return nil

}
