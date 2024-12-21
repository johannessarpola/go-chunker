package chunk

type Message struct {
	idx int
	msg []byte
}

func NewMessage(idx int, msg []byte) Message {
	return Message{idx: idx, msg: msg}
}
