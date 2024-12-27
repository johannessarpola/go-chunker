package chunk

import "sync"

// Arbitrer arbitrates source to multiple channels so that order is preserved with the input data.
type Arbitrer struct {
	workers int
	source  Source[string]
}

// NewArbitrer creates a new arbitrer.
func NewArbitrer(workers int, source Source[string]) *Arbitrer {
	return &Arbitrer{
		workers: workers,
		source:  source,
	}
}

// Run runs the arbitrer.
func (a *Arbitrer) Run(total int64, chans ...chan Message) {
	channelCount := len(chans)
	wg := sync.WaitGroup{}
	wg.Add(a.workers)

	for _ = range a.workers {
		go func() {
			defer wg.Done()
			for {
				val, idx, ok := a.source.Next()
				// dst determines the correct channel to send the message so the order is not shuffled.
				// For example with idx = 0 it would end in the first channel.
				dst := idx / int(total)

				// TODO ugly fix, figure out later
				if dst >= channelCount {
					dst = channelCount - 1
				}

				if !ok {

					return
				}
				chans[dst] <- NewMessage(idx, []byte(val))
			}

		}()
	}

	wg.Wait()
	for _, c := range chans {
		close(c)
	}
}
