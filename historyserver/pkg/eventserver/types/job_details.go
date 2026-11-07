package types

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

// JobDriverInfo is the Ray Jobs API's driver identity, not a submission identity.
type JobDriverInfo struct {
	ID            string `json:"id"`
	NodeIPAddress string `json:"node_ip_address"`
	PID           string `json:"pid"`
}

// JobDetails preserves the nullable fields and millisecond timestamps of the Ray
// Jobs API. It is separate from the event-derived Job schema. ParseJobDetailsList
// discards runtime_env; captured submission records must not retain it.
type JobDetails struct {
	Type                   string            `json:"type"`
	JobID                  *string           `json:"job_id"`
	SubmissionID           *string           `json:"submission_id"`
	DriverInfo             *JobDriverInfo    `json:"driver_info"`
	Status                 JobStatus         `json:"status"`
	EntryPoint             string            `json:"entrypoint"`
	Message                *string           `json:"message"`
	ErrorType              *string           `json:"error_type"`
	StartTime              *int64            `json:"start_time"`
	EndTime                *int64            `json:"end_time"`
	Metadata               map[string]string `json:"metadata"`
	RuntimeEnv             *json.RawMessage  `json:"runtime_env"`
	DriverAgentHTTPAddress *string           `json:"driver_agent_http_address"`
	DriverNodeID           *string           `json:"driver_node_id"`
	DriverExitCode         *int              `json:"driver_exit_code"`
}

// ParseJobDetailsList requires a JSON array and validates rows independently.
// Row errors use zero-based indexes and never include payload values. Valid rows
// are retained for recovery of older snapshots, but a collector must not replace
// its last good snapshot when any row is invalid. RuntimeEnv is always nil.
func ParseJobDetailsList(body []byte) (records []JobDetails, rowErrors []error, err error) {
	body = bytes.TrimSpace(body)
	if len(body) == 0 || body[0] != '[' {
		return nil, nil, fmt.Errorf("jobs response must be a JSON array")
	}
	var rows []json.RawMessage
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, nil, fmt.Errorf("invalid jobs JSON array")
	}

	records = make([]JobDetails, 0, len(rows))
	for i, row := range rows {
		var record JobDetails
		if err := json.Unmarshal(row, &record); err != nil {
			rowErrors = append(rowErrors, fmt.Errorf("row %d: invalid job details fields", i))
			continue
		}
		record.RuntimeEnv = nil
		if err := record.validate(); err != nil {
			rowErrors = append(rowErrors, fmt.Errorf("row %d: %w", i, err))
			continue
		}
		records = append(records, record)
	}
	return records, rowErrors, nil
}

func (j *JobDetails) validate() error {
	switch j.Status {
	case PENDING, JOBRUNNING, SUCCEEDED, JOBFAILED, STOPPED:
	default:
		return fmt.Errorf("invalid job status")
	}

	if j.Type == "" {
		switch {
		case nonemptyJobID(j.SubmissionID):
			j.Type = "SUBMISSION"
		case nonemptyJobID(j.JobID):
			j.Type = "DRIVER"
		}
	}
	switch j.Type {
	case "SUBMISSION":
		if !nonemptyJobID(j.SubmissionID) {
			return fmt.Errorf("submission requires submission_id")
		}
	case "DRIVER":
		if !nonemptyJobID(j.JobID) {
			return fmt.Errorf("driver requires job_id")
		}
	default:
		return fmt.Errorf("invalid job type or missing identity")
	}

	if j.DriverInfo != nil {
		driver := j.DriverInfo
		if !nonemptyJobID(j.JobID) || driver.ID != *j.JobID ||
			strings.TrimSpace(driver.NodeIPAddress) == "" || strings.TrimSpace(driver.PID) == "" {
			return fmt.Errorf("invalid or inconsistent driver_info")
		}
	}
	return nil
}

func nonemptyJobID(id *string) bool {
	return id != nil && strings.TrimSpace(*id) != ""
}

// IsTerminalJobStatus reports statuses a submitted job never leaves.
func IsTerminalJobStatus(status JobStatus) bool {
	return status == SUCCEEDED || status == JOBFAILED || status == STOPPED
}
