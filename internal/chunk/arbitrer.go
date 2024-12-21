package chunk

// Arbitrer arbitrates source to multiple channels so that order is preserved with the input data.
type Arbitrer struct {
	source Source[string]
}

// NewArbitrer creates a new arbitrer.
func NewArbitrer(source Source[string]) *Arbitrer {
	return &Arbitrer{
		source: source,
	}
}

// Run runs the arbitrer.
func (a *Arbitrer) Run(chans ...chan Message) {
	channelCount := len(chans)
	for {
		val, idx, ok := a.source.Next()
		// dst determines the correct channel to send the message so the order is not shuffled.
		// For example with idx = 0 it would end in the first channel.
		dst := (idx / channelCount) % channelCount
		if !ok {
			for _, c := range chans {
				close(c)
			}
			return
		}
		chans[dst] <- Message{msg: []byte(val), idx: idx}
	}

}
