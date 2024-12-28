package chunk

type Message struct {
	idx int64
	msg []byte
}

func NewMessage(idx int64, msg []byte) Message {
	return Message{idx: idx, msg: msg}
}
