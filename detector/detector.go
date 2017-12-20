package detector

import (
	"net/url"
)

type Detector interface {
	// Detect detect the mesos leader.
	Detect() (string, error)
}

func NewDetector(master string) (Detector, error) {
	u, err := url.Parse(master)
	if err != nil {
		return nil, err
	}

	switch u.Scheme {
	case "zk":
		return zk.NewDetector(u.Host), nil
	}
}
