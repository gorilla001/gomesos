package scheduler

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	//mesosproto "gitee.com/escloud/escloud/cluster/mesos/proto"
	"github.com/andygrunwald/megos"
	mesosproto "github.com/mesos/mesos-go/api/v0/mesosproto"
	"github.com/samuel/go-zookeeper/zk"
	log "github.com/sirupsen/logrus"
)

type detector struct {
	ZKHosts []string
	ZKPath  string
}

func newDetector(hosts []string, path string) *detector {
	return &detector{
		ZKHosts: hosts,
		ZKPath:  path,
	}
}

func (d *detector) detect() (string, error) {
	client, err := d.client()
	if err != nil {
		return "", err
	}

	leader, err := client.DetermineLeader()
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", leader.Host, leader.Port), nil
}
