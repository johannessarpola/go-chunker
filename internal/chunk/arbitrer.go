package chunk

type Arbitrer struct {
	source Source[string]
}

func NewArbitrer(source Source[string]) *Arbitrer {
	return &Arbitrer{
		source: source,
	}
}

func (a *Arbitrer) Run(batchSize int, chans ...chan Message) {
	chanLen := len(chans)
	for {
		val, idx, ok := a.source.Next()
		dst := (idx / batchSize) % chanLen
		if !ok {
			for _, c := range chans {
				close(c)
			}
			return
		}
		chans[dst] <- Message{msg: []byte(val), idx: idx}
	}

}
