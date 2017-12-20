package scheduler

import (
	"net/http"

	mesos "github.com/pwzgorilla/mfl/mesosproto"
)

type session struct {
	closed     chan struct{}
	registered chan struct{}
	events     *mesos.Event
	errs       chan error
	client     *http.Client
	driver     *MesosSchedulerDriver
	streamID   string
	master     string
	stream     *stream
}

func newSession(driver *MesosSchedulerDriver) *session {
	return &session{
		client: &http.Client{
			Transport: &http.Transport{
				Dial: (&net.Dialer{
					Timeout:   10 * time.Second,
					KeepAlive: 30 * time.Second,
				}).Dial,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 10 * time.Second,
				Timeout:               10 * time.Second,
			},
		},
		events:     make(chan *mesos.Event, 1024),
		stopped:    make(chan struct{}),
		registered: make(chan struct{}),
		driver:     driver,
	}
}

func (s *session) start() error {
	master, err := s.detector.Detect()
	if err != nil {
		return err
	}

	s.master = master

	go s.listen(resp)
}

func (s *session) register() error {
	req, err := makeRequest(msg)
	if err != nil {
		return err
	}

	var (
		stream *stream
		event  *mesos.Event
		err    error
	)

	chErr := make(chan error, 1)

	go func() {
		resp, err := c.client.Do(req)
		if err != nil {
			chErr <- err
			return
		}

		if resp.Header.Get("Mesos-Stream-Id") != "" {
			s.streamID = resp.Header.Get("Mesos-Stream-Id")
		}

		stream := newStream(resp.Body)

		event, err := stream.recv()
		chErr <- err
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

	return nil
}

func (s *session) listen() error {
	for {
		event, err := s.stream.recv()
		if err != nil {
			return err
		}

		hanlder, ok := s.driver.handlers[event.GetType()]
		if !ok {
			continue
		}

		hanlder(event)
	}
}

func (s *session) listen(resp *http.Response) {
	defer resp.Body.Close()

	dec := json.NewDecoder(NewReader(resp.Body))

	for {
		select {
		case <-s.registered:

		case <-t.stop:
			return nil
		case err := <-s.errs:
			if err != nil {
				s.close()
				go s.register()
				return
			}
		default:
			if err = dec.Decode(&event); err != nil {
				s.errs <- err
			}

			handler, ok := s.driver.handlers[event.GetType()]
			if !ok {
				continue
			}

			handler(event)
		}
	}

}

func (s *session) send(msg *Message) error {
	req, err := makeRequest(msg)
	if err != nil {
		return err
	}

	if s.streamID != "" {
		req.Header.Set("Mesos-Stream-Id", c.streamID)
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

func (s *session) makeRequest(msg *Message) (*http.Request, error) {
	req, err := http.NewRequest("POST", s.master, bytes.NewReader(msg.Bytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/json")

	return req, nil
}
