package main

import (
	"fmt"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"time"

	mesos "github.com/mesos/go-proto/mesos/v1"
	driver "github.com/pwzgorilla/libmesos/scheduler"
)

type scheduler struct {
	driver driver.SchedulerDriver
	offers []*mesos.Offer
}

func (s *scheduler) Registered(driver driver.SchedulerDriver, frameworkId *mesos.FrameworkID, masterIfo *mesos.MasterInfo) {
	log.Println("Registered")
}

func (s *scheduler) HeartBeated() {
	log.Println("Heartbeated")
}

func (s *scheduler) ResourceOffers(driver driver.SchedulerDriver, offers []*mesos.Offer) {

	for _, offer := range offers {
		s.offers = append(s.offers, offer)
	}

	for _, offer := range s.offers {
		fmt.Println(offer.GetId().GetValue(), "===", offer)
	}

}

func (s *scheduler) OfferRescinded(driver driver.SchedulerDriver, offerID *mesos.OfferID) {
}

func (s *scheduler) StatusUpdate(driver driver.SchedulerDriver, status *mesos.TaskStatus) {
}

func (s *scheduler) FrameworkMessage(driver driver.SchedulerDriver, agentId *mesos.AgentID, executorId *mesos.ExecutorID, data []byte) {
}

func (s *scheduler) FailureMessage(driver driver.SchedulerDriver, agentId *mesos.AgentID, executorId *mesos.ExecutorID, status *int32) {
}

func (s *scheduler) ErrorMessage(driver driver.SchedulerDriver, message *string) {
}

var task = &mesos.TaskInfo{
	Name: proto.String(fmt.Sprintf("%d", time.Now().Nanosecond())),
	TaskId: &mesos.TaskID{
		Value: proto.String(fmt.Sprintf("%d", time.Now().Nanosecond())),
	},
	AgentId: &mesos.AgentID{
		Value: proto.String("ad2e6e06-3f3d-4585-a649-9d424d8d8aac-S0"),
	},
	Resources: []*mesos.Resource{
		{
			Name: proto.String("cpus"),
			Type: mesos.Value_SCALAR.Enum(),
			Scalar: &mesos.Value_Scalar{
				Value: proto.Float64(0.1),
			},
		},
		{
			Name: proto.String("mem"),
			Type: mesos.Value_SCALAR.Enum(),
			Scalar: &mesos.Value_Scalar{
				Value: proto.Float64(5),
			},
		},
	},
	Command: &mesos.CommandInfo{Shell: proto.Bool(false)},
	Container: &mesos.ContainerInfo{
		Type: mesos.ContainerInfo_DOCKER.Enum(),
		Docker: &mesos.ContainerInfo_DockerInfo{
			Image:          proto.String("nginx"),
			Network:        mesos.ContainerInfo_DockerInfo_BRIDGE.Enum(),
			ForcePullImage: proto.Bool(false),
		},
	},
}

func main() {
	scheduler := new(scheduler)

	framework := &mesos.FrameworkInfo{
		//Id: &mesos.FrameworkID{
		//	Value: proto.String("d857fe70-268d-44b3-9ec1-56e90f5cca24-0000"),
		//},
		Name:            proto.String("libmesos"),
		User:            proto.String("root"),
		FailoverTimeout: proto.Float64(86400),
	}

	cfg := driver.DriverConfig{
		Scheduler: scheduler,
		Framework: framework,
		Master:    "zk://192.168.1.95:2181/mesos",
	}

	driver, err := driver.NewMesosSchedulerDriver(cfg)
	if err != nil {
		log.Println(err)
		return
	}

	scheduler.driver = driver

	go func() {
		time.Sleep(10 * time.Second)

		log.Println("Launch tasks....")

		offerIds := []*mesos.OfferID{}

		for _, offer := range scheduler.offers {
			offerIds = append(offerIds, offer.GetId())
		}

		if len(offerIds) > 0 {
			if err := scheduler.driver.LaunchTasks(offerIds, []*mesos.TaskInfo{task}, &mesos.Filters{}); err != nil {
				log.Println("Launch error", err)
			}
		}

	}()

	if err := driver.Start(); err != nil {
		log.Println("Start error:", err)
	}

}
