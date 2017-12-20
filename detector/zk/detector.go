package zk

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
	hosts string
}

func NewDetector(hosts string) *detector {
	return &detector{
		hosts: hosts,
	}
}

func (d *detector) Detect() (string, error) {
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

func (d *detector) client() (*megos.Client, error) {
	var (
		hosts   = strings.Split(d.hosts, ",")
		timeout = time.Second * 10
	)

	conn, connCh, err := zk.Connect(hosts, timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// waiting for zookeeper to be connected.
	for event := range connCh {
		if event.State == zk.StateConnected {
			log.Info("connected to zookeeper succeed.")
			break
		}
	}

	var (
		masters    = make([]*url.URL, 0)
		masterInfo = new(mesosproto.MasterInfo)
	)

	children, _, err := conn.Children(d.ZKPath)
	if err != nil {
		return nil, fmt.Errorf("get children on %s error: %v", d.ZKPath, err)
	}
	for _, node := range children {
		if !strings.HasPrefix(node, "json.info") {
			continue
		}

		path := d.ZKPath + "/" + node
		data, _, err := conn.Get(path)
		if err != nil {
			return nil, fmt.Errorf("get node on %s error: %v", path, err)
		}
		if err := json.Unmarshal(data, masterInfo); err != nil {
			return nil, err
		}

		address := masterInfo.GetAddress()
		masters = append(masters, &url.URL{
			Scheme: "http",
			Host:   fmt.Sprintf("%s:%d", address.GetIp(), address.GetPort()),
		})
	}

	return megos.NewClient(masters, nil), nil
}
