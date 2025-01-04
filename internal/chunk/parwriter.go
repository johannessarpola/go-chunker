package chunk

import (
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

type ParWriterMeta struct {
	Source       string   `json:"source"`
	OutputFiles  []string `json:"output_files"`
	Duration     string   `json:"duration"`
	Total        int64    `json:"total"`
	Size         int64    `json:"size"`
	NumWorkers   int      `json:"num_workers"`
	NumArbitrers int      `json:"num_arbiters"`
}

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
		channels[i] = make(chan Message, 1)
	}
	return channels
}

func initializeWorkers(output Output, chans []chan Message) ([]*WriteWorker, error) {
	writers := make([]*WriteWorker, len(chans))
	var err error
	for i, c := range chans {
		writers[i], err = NewWriteWorker(i, c, output)
		if err != nil {
			return nil, fmt.Errorf("failed to create writer: %w", err)
		}
	}
	return writers, nil

}

// TODO Fix the typings at some point
func (np *ParWriter) Run(source Source[string], output Output) error {
	start := time.Now()
	chans := initializeChannels(np.workers)

	writers, err := initializeWorkers(output, chans)
	if err != nil {
		return fmt.Errorf("failed to create workers: %w", err)
	}

	arbiterCount := np.workers / 2 // Let's assume 1 arbitrer per 2 workers
	if arbiterCount <= 0 {
		// Fallback to 1
		arbiterCount = 1
	}
	arbitrer := NewArbitrer(arbiterCount, source)

	// start writers
	wg := sync.WaitGroup{}
	wg.Add(len(writers))

	// progress bar
	p := mpb.New(mpb.WithWaitGroup(&wg))

	wt := np.total / int64(len(writers))

	for _, worker := range writers {
		bar := p.AddBar(wt,
			mpb.PrependDecorators(
				decor.Name(worker.file.Name(), decor.WC{C: decor.DindentRight | decor.DextraSpace}),
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

	arbitrer.Run(wt, chans...)

	p.Wait()
	end := time.Now()

	// Add a separte waitgroup for background tasks
	bgtasks := &sync.WaitGroup{}
	bgtasks.Add(1)

	// Write the metadata file
	go func() {
		defer bgtasks.Done()

		of := []string{}
		for _, f := range writers {
			of = append(of, f.OutputFIle())
		}
		pwm := ParWriterMeta{
			Source:       source.ID(),
			OutputFiles:  of,
			Duration:     end.Sub(start).String(),
			Total:        np.total,
			Size:         wt,
			NumWorkers:   np.workers,
			NumArbitrers: arbiterCount,
		}
		op := path.Join(output.Dir, fmt.Sprintf("%s_meta.json", source.ID()))
		err = WriteJson(op, true, &pwm)
		if err != nil {
			fmt.Println("Error writing meta data to file", err)
		}
	}()

	// Wait for all the tasks to finish
	bgtasks.Wait()

	return nil

}
