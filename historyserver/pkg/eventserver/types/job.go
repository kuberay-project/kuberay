package types

import (
	"maps"
	"sync"
	"time"
)

// JobStatus represents the job submission progress and execution status
type JobStatus string

const (
	SUCCEEDED JobStatus = "SUCCEEDED"
	PENDING   JobStatus = "PENDING"
	STOPPED   JobStatus = "STOPPED"
	// FAILED is already used in the package, using JOBFAILED
	JOBFAILED JobStatus = "FAILED"
	// RUNNING is already used in the package, using JOBRUNNING
	JOBRUNNING JobStatus = "RUNNING"
)

type JobStatusTransition struct {
	Status    JobStatus
	Timestamp time.Time
}

// JobState represents the literal job driver process and connectivity
type JobState string

const (
	UNSPECIFIED JobState = "UNSPECIFIED"
	CREATED     JobState = "CREATED"
	// FINISHED is already used in package, using JOBFINSIHED
	JOBFINISHED JobState = "FINISHED"
)

type JobStateTransition struct {
	State     JobState
	Timestamp time.Time
}

type DriverJobConfig struct {
	Metadata             map[string]string `json:"metadata"`
	SerializedRuntimeEnv string            `json:"serializedRuntimeEnv"`
}

type Job struct {
	// JobID will be in Hex, as that is the output format when calling /api/jobs/
	JobID                  string            `json:"jobId"`
	SubmissionID           string            `json:"submissionId"`
	JobType                string            `json:"type"`
	Status                 JobStatus         `json:"status"`
	State                  JobState          `json:"state"`
	EntryPoint             string            `json:"entrypoint"`
	Message                string            `json:"message"`
	ErrorType              string            `json:"errorType"`
	StartTime              time.Time         `json:"startTime"`
	EndTime                time.Time         `json:"endTime"`
	Config                 DriverJobConfig   `json:"config"`
	RuntimeEnv             map[string]string `json:"runtimeEnv"`
	DriverAgentHttpAddress string            `json:"driverAgentHttpAddress"`
	DriverNodeID           string            `json:"driverNodeId"`
	DriverNodeIPAddress    string            `json:"driverNodeIpAddress"`
	DriverExitCode         int               `json:"driverExitCode"`
	DriverPID              string            `json:"driverPid"`

	// StateTransitions is the state (connectivity of the driver) job timeline
	StateTransitions []JobStateTransition

	// StatusTransitions is the status (progress) of the job timeline
	StatusTransitions []JobStatusTransition
}

type JobMap struct {
	JobMap map[string]Job
	Mu     sync.Mutex
}

func (j *JobMap) Lock() {
	j.Mu.Lock()
}

func (j *JobMap) Unlock() {
	j.Mu.Unlock()
}

func NewJobMap() *JobMap {
	return &JobMap{
		JobMap: make(map[string]Job),
	}
}

type ClusterJobMap struct {
	ClusterJobMap map[string]*JobMap
	Mu            sync.RWMutex
}

func (c *ClusterJobMap) RLock() {
	c.Mu.RLock()
}

func (c *ClusterJobMap) RUnlock() {
	c.Mu.RUnlock()
}

func (c *ClusterJobMap) Lock() {
	c.Mu.Lock()
}

func (c *ClusterJobMap) Unlock() {
	c.Mu.Unlock()
}

func (c *ClusterJobMap) GetOrCreateJobMap(clusterName string) *JobMap {
	c.Lock()
	defer c.Unlock()

	jobMap, exists := c.ClusterJobMap[clusterName]
	if !exists {
		jobMap = NewJobMap()
		c.ClusterJobMap[clusterName] = jobMap
	}

	return jobMap
}

// CreateOrMergeJob will find or create the job and runs the merge function.
// There are two fields that is part of the Job object that can transition.
// The State and Status, one representing the driver lifecycle and another
// represents the status of the progress of the job.
func (j *JobMap) CreateOrMergeJob(jobId string, mergeFn func(*Job)) {
	j.Lock()
	defer j.Unlock()

	job, exist := j.JobMap[jobId]
	if !exist {
		// Job does not exist, creating new with JobID
		newJob := Job{JobID: jobId}
		mergeFn(&newJob)
		j.JobMap[jobId] = newJob
		return
	}

	mergeFn(&job)
	j.JobMap[jobId] = job
}

// DeepCopy will return a deep copy of the Job
func (j Job) DeepCopy() Job {
	cp := j
	if len(j.StateTransitions) > 0 {
		cp.StateTransitions = make([]JobStateTransition, len(j.StateTransitions))
		copy(cp.StateTransitions, j.StateTransitions)
	}
	if len(j.StatusTransitions) > 0 {
		cp.StatusTransitions = make([]JobStatusTransition, len(j.StatusTransitions))
		copy(cp.StatusTransitions, j.StatusTransitions)
	}
	if len(j.Config.Metadata) > 0 {
		cp.Config.Metadata = make(map[string]string, len(j.Config.Metadata))
		maps.Copy(cp.Config.Metadata, j.Config.Metadata)
	}
	if len(j.RuntimeEnv) > 0 {
		cp.RuntimeEnv = make(map[string]string, len(j.RuntimeEnv))
		maps.Copy(cp.RuntimeEnv, j.RuntimeEnv)
	}

	return cp
}
