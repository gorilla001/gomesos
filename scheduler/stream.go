package scheduler

import (
	"encoding/json"
	"io"

	sched "github.com/mesos/go-proto/mesos/v1/scheduler"
	log "github.com/sirupsen/logrus"
)

type stream struct {
	stream  io.ReadCloser
	decoder *json.Decoder
	closed  chan struct{}
}

func newStream(body io.ReadCloser) *stream {
	return &stream{
		stream:  body,
		decoder: json.NewDecoder(NewReader(body)),
		closed:  make(chan struct{}),
	}
}

func (s *stream) recv() (*sched.Event, error) {
	var event *sched.Event

	select {
	case <-s.closed:
		return nil, nil
	default:
		if err := s.decoder.Decode(&event); err != nil {
			log.Errorf("Decode mesos event got error: %v", err)

			return nil, err
		}
	}

	return event, nil
}

func (s *stream) close() {
	s.stream.Close()
	close(s.closed)
}
