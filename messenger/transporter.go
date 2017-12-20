package main

type HTTPTransporter struct {
	tr       *http.Transport
	client   *http.Client
	chEvent  chan *mesos.Event
	streamID string
	stop     chan struct{}
}

func NewHTTPTransporter(ch chan *mesos.Event) *HTTPTransporter {
	return &HTTPTransport{
		tr: &http.Transport{
			Dial: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   10 * time.Second,
			ResponseHeaderTimeout: 10 * time.Second,
		},
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
		chEvent: make(chan *mesos.Event, 1024),
		stop:    make(chan struct{}),
	}
}

func (t *HTTPTransporter) register(msg []byte, errs chan error) error {
	req, err := makeRequest(msg)
	if err != nil {
		return err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}

	if resp.Header.Get("Mesos-Stream-Id") != "" {
		t.streamID = resp.Header.Get("Mesos-Stream-Id")
	}

	go func() {
		errs <- t.decode(resp)
	}()

	return nil
}

func (t *HTTPTransporter) decode(resp *http.Response) error {
	defer resp.Body.Close()

	dec := json.NewDecoder(NewReader(resp.Body))

	for {
		select {
		case <-t.stop:
			return nil
		default:
			if err = dec.Decode(&ev); err != nil {
				log.Errorf("mesos events subscriber decode events error: %v", err)
				resp.Body.Close()

				return err
			}

			t.chEvent <- ev
		}
	}
}

func (t *HTTPTransporter) send(msg *Message) error {
	req, err := makeRequest(msg)
	if err != nil {
		return err
	}

	if t.streamID != "" {
		req.Header.Set("Mesos-Stream-Id", c.streamID)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("Unable to do request: %s", err)
	}

	if (resp.StatusCode != http.StatusOK) && (resp.StatusCode != http.StatusAccepted) {
		return fmt.Errorf("request is send but status code not correct %d", resp.StatusCode)
	}

	return nil
}

func (t *HTTPTransporter) recv() (*mesos.Event, error) {
	select {
	case event := <-t.chEvent:
		return event, nil
	case <-t.stop:

	}

	return nil, fmt.Errorf("transport stopped")
}

func (t *HTTPTransporter) stop() {
	close(t.stop)
}

func makeRequest(msg *Message) (*http.Request, error) {
	req, err := http.NewRequest("POST", msg.Destination, bytes.NewReader(msg.Bytes))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("Accept", "application/json")

	return req, nil
}
