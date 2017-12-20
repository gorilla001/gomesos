package scheduler

import (
	"io"
	"net/http"
)

type stream struct {
	body    io.ReadCloser
	decoder io.Reader
	closed  chan struct{}
}

func newStream(body io.ReadCloser) *stream {
	return &stream{
		body:    body,
		decoder: json.NewDecoder(NewReader(body)),
		closed:  make(chan struct{}),
	}
}

func (s *stream) recv() (*mesos.Event, error) {
	var event *mesos.Event

	select {
	case <-s.closed:
		return nil, nil
	default:
		if err := s.decoder.Decode(&event); err != nil {
			return nil, err
		}
	}

	return event, nil
}

func (s *stream) close() {
	s.body.Close()
	close(s.closed)
}
