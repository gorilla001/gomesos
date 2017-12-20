package mfl

type connector struct {
	leader string
}

func newConnector(leader string) *connector {
	return &connector{
		leader: leader,
	}
}

func (c *connector) connect() {
	call := &sched.Call{
		Type: sched.Call_SUBSCRIBE.Enum(),
		Subscribe: &sched.Call_Subscribe{
			FrameworkInfo: c.frameworkInfo,
		},
		FrameworkId: c.frameworkInfo.Id,
	}

	// If we disconnect we need to reset the stream ID. For this reason always start with a fresh stream ID.
	// Otherwise we'll never be able to reconnect.
	c.Client.SetStreamID("")

	resp, err := c.Client.Request(call)
	if err != nil {
		return resp, err
	} else {
		// recordio.Decode() returns an err struct
		return resp, recordio.Decode(resp.Body, eventChan)
	}
}
