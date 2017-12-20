package messenger

const (
	defaultQueueSize = 1024
)

type dispatchFunc func(*mesos.Call) error

type eventHandler func(*messo.Event)

type MesosMessenger struct {
	http     *httpClient
	detector *detector
	closed   chan struct{}

	sendingQueue chan dispatchFunc
	handlers     map[string]eventHandler
}

func NewMessenger(master string) *MesosMessenger {
	return &MesosMessenger{
		sendingQueue: make(chan dispatchFunc, defaultQueueSize),
		handlers:     make(map[string]eventHandler),
	}
}

func (m *MesosMessenger) Install(kind string, handler eventHandler) {
	m.handlers[kind] = handler
}

func (m *MesosMessenger) Send(call *mesos.Call) error {
	payload, err := proto.Marshal(call)
	if err != nil {
		return err
	}

	d := func()

	m.sendingQueue <- d

	return nil
}

func (m *MesosMessenger) Route(event *mesos.Event) error {
	handler, ok := m.handlers[event.GetType()]
	if !ok {
		return fmt.Errorf("Failed to route message")
	}

	go handler(event)

	return nil
}

func (m *MesosMessenger) Start() error {
}

func (m *MesosMessenger) Start() error {
	call := &mesosproto.Call{
		Type: mesosproto.Call_SUBSCRIBE.Enum(),
		Subscribe: &mesosproto.Call_Subscribe{
			FrameworkInfo: s.framework,
		},
	}

	resp, err := m.Send(call)
	if err != nil {
		return err
	}

	if d := resp.StatusCode; d != http.StatusOK {
		return fmt.Errorf("subscribed call return %d", d)
	}

	go handle(resp)

	return nil
}

func (m *MesosMessenger) Send(call *mesosproto.Call) {
}

func (m *MesosMessenger) handle(resp *http.Response) {
	var (
		ev  *mesosproto.Event
		err error
	)

	dec := json.NewDecoder(NewReader(resp.Body))

	for {
		select {
		case <-m.closed:
			resp.Body.Close()
			return
		default:
			if err = dec.Decode(&ev); err != nil {
				log.Errorf("mesos events subscriber decode events error: %v", err)
				resp.Body.Close()
				go s.reconnect()
				return
			}

			s.handleEvent(ev)
		}
	}

}

func (m *MesosMessenger) Start() {
	session := newSession(m.master)

	if err := session.Start(); err != nil {
		return err
	}

	for {
		select {
		case <-m.stop:
			return
		case <-m.errs:
			session = newSession(m.master)

		}
	}
}

func (m *MesosMessenger) Stop() {
	close(m.closed)
}

func (m *MesosMessenger) decodeLoop() {
	for {
		select {
		case <-m.stop:
			return
		default:
		}

		msg, err := m.tr.Recv()
		if err != nil {
			return
		}

		handler, found := m.eventHandlers[msg.GetType()]
		if !found {
			return
		}

		handler(msg)
	}
}
