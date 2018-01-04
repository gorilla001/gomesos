package detector

import (
	"fmt"
	"net/url"

	"github.com/pwzgorilla/gomesos/detector/zk"
	log "github.com/sirupsen/logrus"
)

type Detector interface {
	// Detect detect the mesos leader.
	Detect() (string, error)
}

func NewDetector(master string) (Detector, error) {
	u, err := url.Parse(master)
	if err != nil {
		log.Errorf("detector parse error: %v", err)

		return nil, err
	}

	switch u.Scheme {
	case "zk":
		return zk.NewDetector(u.Host, u.Path), nil
	}

	return nil, fmt.Errorf("Unkonw %s", u.Scheme)
}
