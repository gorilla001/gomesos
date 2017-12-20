package mfl

import (
	"net/http"
)

type stream struct {
	conn net.Conn
}

func newStream(conn net.Conn) *stream {
	return &stream{conn: conn}
}

func (s *stream) Recv() (event *mesos.Event, err error) {
	dec := json.NewDecoder(NewReader(conn))

	if err = dec.Decode(&event); err != nil {
		return nil, err
	}

	return
}

func (s *stream) Close() {
	conn.Close()
}
