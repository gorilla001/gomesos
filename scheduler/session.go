package scheduler

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/gogo/protobuf/proto"
	sched "github.com/mesos/go-proto/mesos/v1/scheduler"
	"github.com/pwzgorilla/libmesos/detector"
)

type session struct {
	closed   chan struct{}
	events   chan *sched.Event
	errs     chan error
	client   *http.Client
	driver   *MesosSchedulerDriver
	streamID string
	master   string
	stream   *stream
}

func newSession(driver *MesosSchedulerDriver, detector detector.Detector) *session {
	s := &session{
		client: &http.Client{
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 30 * time.Second,
				}).Dial,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 10 * time.Second,
			},
		},
		events: make(chan *sched.Event),
		closed: make(chan struct{}),
		errs:   make(chan error),
		driver: driver,
	}

	master, err := detector.Detect()
	if err != nil {
		s.errs <- err
		return s
	}

	s.master = master

	go s.start()

	return s
}

func (s *session) start() {
	if err := s.register(); err != nil {
		s.errs <- err
		return
	}

	go func() {
		s.errs <- s.listen()
	}()
}

func (s *session) register() error {
	call := &sched.Call{
		Type: sched.Call_SUBSCRIBE.Enum(),
		Subscribe: &sched.Call_Subscribe{
			FrameworkInfo: s.driver.framework,
		},
	}

	msg, err := proto.Marshal(call)
	if err != nil {
		return err
	}

	req, err := s.makeRequest(msg)
	if err != nil {
		return err
	}

	var (
		stream *stream
		event  *sched.Event
		resp   *http.Response
		errs   error
	)

	chErr := make(chan error, 1)

	go func() {
		resp, errs = s.client.Do(req)
		if errs != nil {
			chErr <- errs
			return
		}

		if resp.Header.Get("Mesos-Stream-Id") != "" {
			s.streamID = resp.Header.Get("Mesos-Stream-Id")
		}

		stream = newStream(resp.Body)

		event, errs = stream.recv()
		chErr <- errs
	}()

	select {
	case err := <-chErr:
		if err != nil {
			return err
		}
	case <-time.After(60 * time.Second):
		return fmt.Errorf("register to mesos master timeout")
	}

	s.stream = stream

	s.events <- event

	return nil
}

func (s *session) listen() error {
	for {
		select {
		case <-s.closed:
			return nil
		default:
			event, err := s.stream.recv()
			if err != nil {
				return err
			}

			s.events <- event
		}
	}
}

func (s *session) send(msg []byte) error {
	req, err := s.makeRequest(msg)
	if err != nil {
		return err
	}

	if s.streamID != "" {
		req.Header.Set("Mesos-Stream-Id", s.streamID)
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("Unable to do request: %s", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("request is send but status code not correct %d", resp.StatusCode)
	}

	return nil

}

func (s *session) makeRequest(msg []byte) (*http.Request, error) {
	req, err := http.NewRequest("POST", "http://"+s.master+"/api/v1/scheduler", bytes.NewReader(msg))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/json")

	return req, nil
}

func (s *session) close() {
	if s.stream != nil {
		s.stream.close()
	}

	close(s.closed)
}
