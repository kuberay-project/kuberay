package historyserver

import (
	"sort"

	"github.com/sirupsen/logrus"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

// jobReplayView is request-local. Core records in the snapshot remain untouched
// for tasks, actors and other endpoints.
// Conflicts is a body-free diagnostic count, leaving construction side-effect free.
type jobReplayView struct {
	Rows      []any
	byCore    map[string]any
	bySubmit  map[string]any
	Conflicts int
}

func (v jobReplayView) Lookup(id string) (any, bool) {
	if row, ok := v.byCore[id]; ok {
		return row, true
	}
	row, ok := v.bySubmit[id]
	return row, ok
}

func lookupJobReplay(snap *eventserver.SessionSnapshot, id string) (any, bool) {
	job, eventExists := snap.Jobs[id]
	if eventExists {
		if submissionID := eventSubmissionID(job); submissionID != "" {
			if record, ok := snap.Submissions[submissionID]; ok && record.JobID != nil && *record.JobID == id {
				row, _ := mergeSubmission(record, &job)
				return row, true
			}
			return formatJobForResponse(job), true
		}
	}

	// Reverse lookup is only needed for missing event metadata or REST-only cores.
	// Match the list view's core precedence and ordering for ambiguous REST references.
	var coreRecord eventtypes.JobDetails
	var coreSubmissionID string
	for submissionID, record := range snap.Submissions {
		if record.JobID != nil && *record.JobID == id && id != "" && submissionID > coreSubmissionID {
			coreRecord, coreSubmissionID = record, submissionID
		}
	}
	if coreSubmissionID != "" {
		var driver *eventtypes.Job
		if eventExists {
			driver = &job
		}
		row, _ := mergeSubmission(coreRecord, driver)
		return row, true
	}
	if eventExists {
		return formatJobForResponse(job), true
	}

	record, ok := snap.Submissions[id]
	if !ok {
		return nil, false
	}
	var driver *eventtypes.Job
	if record.JobID != nil && *record.JobID != "" {
		if job, ok := snap.Jobs[*record.JobID]; ok {
			if submissionID := eventSubmissionID(job); submissionID == "" || submissionID == id {
				driver = &job
			}
		}
	}
	row, _ := mergeSubmission(record, driver)
	return row, true
}

func eventSubmissionID(job eventtypes.Job) string {
	if job.SubmissionID != "" {
		return job.SubmissionID
	}
	return job.Config.Metadata["job_submission_id"]
}

// logJobReplayConflicts reports once per session load that REST and event data
// disagree on a terminal outcome. The jobs list endpoint is polled, so emitting
// this per request would repeat the same data-quality signal on every response.
func logJobReplayConflicts(session utils.ClusterInfo, snap *eventserver.SessionSnapshot) {
	if len(snap.Submissions) == 0 {
		return
	}
	if conflicts := buildJobReplayView(snap).Conflicts; conflicts > 0 {
		sessionKey := utils.BuildClusterSessionKey(session.Name, session.Namespace, session.SessionName)
		logrus.Warnf("Jobs replay for %s: %d conflicting explicit terminal outcomes; using REST submission outcomes",
			sessionKey, conflicts)
	}
}

func buildJobReplayView(snap *eventserver.SessionSnapshot) jobReplayView {
	v := jobReplayView{Rows: make([]any, 0), byCore: make(map[string]any), bySubmit: make(map[string]any)}
	// Submission records are validated when the session is loaded.
	submissions := snap.Submissions
	for core, job := range snap.Jobs {
		v.byCore[core] = formatJobForResponse(job)
	}
	submitIDs := make([]string, 0, len(submissions))
	for id := range submissions {
		submitIDs = append(submitIDs, id)
	}
	sort.Strings(submitIDs)
	mergedCores := make(map[string]bool)
	for _, id := range submitIDs {
		record := submissions[id]
		core := ""
		if record.JobID != nil {
			core = *record.JobID
		}
		// Only the core ID captured by REST can associate an event driver.
		// Submission metadata alone must not infer or select another driver.
		var driver *eventtypes.Job
		job, eventExists := snap.Jobs[core]
		if core != "" && eventExists {
			if submissionID := eventSubmissionID(job); submissionID == "" || submissionID == id {
				driver = &job
			}
		}
		row, conflict := mergeSubmission(record, driver)
		if conflict {
			v.Conflicts++
		}
		v.Rows = append(v.Rows, row)
		v.bySubmit[id] = row
		if core != "" && (!eventExists || driver != nil) {
			v.byCore[core] = row
			mergedCores[core] = true
		}
	}
	// Unmatched event jobs keep their individual legacy rows and core lookups.
	for core := range snap.Jobs {
		if !mergedCores[core] {
			v.Rows = append(v.Rows, v.byCore[core])
		}
	}
	return v
}

// Only explicit progress status is evidence of a terminal submission outcome.
// FINISHED describes driver connectivity, not success of the submission.
func explicitJobStatus(job eventtypes.Job) eventtypes.JobStatus {
	status := job.Status
	if n := len(job.StatusTransitions); n > 0 && job.StatusTransitions[n-1].Status != "" {
		status = job.StatusTransitions[n-1].Status
	}
	return status
}

func mergeSubmission(record eventtypes.JobDetails, job *eventtypes.Job) (eventtypes.JobDetails, bool) {
	record.RuntimeEnv = nil
	if job == nil {
		return record, false
	}
	if job.DriverNodeIPAddress != "" && job.DriverPID != "" {
		record.DriverInfo = &eventtypes.JobDriverInfo{ID: job.JobID, NodeIPAddress: job.DriverNodeIPAddress, PID: job.DriverPID}
	}
	if job.DriverNodeID != "" {
		record.DriverNodeID = new(job.DriverNodeID)
	}
	if job.DriverAgentHttpAddress != "" {
		record.DriverAgentHTTPAddress = new(job.DriverAgentHttpAddress)
	}
	if record.StartTime == nil && !job.StartTime.IsZero() {
		record.StartTime = new(job.StartTime.UnixMilli())
	}
	eventStatus := explicitJobStatus(*job)
	restTerminal := eventtypes.IsTerminalJobStatus(record.Status)
	eventTerminal := eventtypes.IsTerminalJobStatus(eventStatus)
	conflict := restTerminal && eventTerminal && record.Status != eventStatus
	if !restTerminal && eventTerminal {
		// These outcome fields must move together when events supply the terminal.
		record.Status = eventStatus
		record.Message = nil
		record.ErrorType = nil
		record.EndTime = nil
		record.DriverExitCode = nil
		if job.Message != "" {
			record.Message = new(job.Message)
		}
		if job.ErrorType != "" {
			record.ErrorType = new(job.ErrorType)
		}
	} else if !restTerminal && eventStatus == eventtypes.JOBRUNNING {
		record.Status = eventtypes.JOBRUNNING
	}
	// Event exit codes are plain integers, so zero may mean an omitted value.
	// Only borrow nonzero exit codes; an explicit REST zero is retained. Like
	// end times, an exit code is only evidence once the outcome is terminal:
	// a still-running driver cannot have exited.
	if !conflict && eventTerminal && eventStatus == record.Status {
		if record.EndTime == nil && !job.EndTime.IsZero() {
			record.EndTime = new(job.EndTime.UnixMilli())
		}
		if record.DriverExitCode == nil && job.DriverExitCode != 0 {
			record.DriverExitCode = new(job.DriverExitCode)
		}
	}
	return record, conflict
}
