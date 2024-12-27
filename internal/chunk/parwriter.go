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
	total   int64
}

func NewParWriter(workers int, total int64) *ParWriter {
	return &ParWriter{workers: workers, total: total}
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

	workerTotal := np.total / int64(len(writers))

	for _, worker := range writers {
		name := fmt.Sprintf("writer-%d:", worker.id)
		bar := p.AddBar(int64(workerTotal),
			mpb.PrependDecorators(
				decor.Name(name, decor.WC{C: decor.DindentRight | decor.DextraSpace}),
				decor.CountersNoUnit("%d / %d", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(
				decor.OnComplete(
					decor.EwmaETA(decor.ET_STYLE_GO, 30, decor.WCSyncWidth), "done",
				),
			),
		)

		start := time.Now()
		// run the writer
		go worker.Run(
			func(m *Message) {
				bar.EwmaIncrement(time.Since(start))
			},
			func(w *WriteWorker, err error) {
				//fmt.Printf("worker %d done, wrote to %s\n", w.id, w.file.Name())
				wg.Done()
			},
		)
	}

	arbitrer.Run(workerTotal, chans...)

	p.Wait()
	return nil

}
