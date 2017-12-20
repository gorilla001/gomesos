package messenger

type Message struct {
	Destination string
	Name        string
	Bytes       []byte
}
