package batchscheduler

import schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"

// NewSchedulerManagerForTest creates a SchedulerManager with a custom BatchScheduler for testing purposes.
func NewSchedulerManagerForTest(scheduler schedulerinterface.BatchScheduler) *SchedulerManager {
	return &SchedulerManager{
		scheduler: scheduler,
	}
}
