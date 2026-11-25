package historyserver

// GetLogFileOptions contains all options for fetching log files
type GetLogFileOptions struct {
	// Node identification (one of these is required if not using task_id/actor_id)
	NodeID string // The node id where the log file is located
	NodeIP string // The node ip address (will be resolved to node_id)

	// Log file identification (provide one of: Filename, TaskID, ActorID, PID)
	Filename string // The log file name (explicit path)
	TaskID   string // Task ID to resolve log file
	ActorID  string // Actor ID to resolve log file
	PID      int    // Process ID to resolve log file

	// Optional parameters with defaults
	// Number of lines to return, default to DEFAULT_LOG_LIMIT (1000)
	// -1 = all lines
	Lines int
	// Timeout in seconds for the request, default to 0 (no timeout)
	Timeout int
	// Attempt number for task retries, default to 0 (first attempt)
	AttemptNumber int
	// Whether to filter ANSI escape codes from logs, default to False
	FilterAnsiCode bool
	// Filename used in the Content-Disposition header to trigger file download.
	// Defaults to DEFAULT_DOWNLOAD_FILENAME when not explicitly provided by the user.
	DownloadFilename string
	// The suffix of the log file ("out" or "err"), default to "out"
	// Used when resolving by TaskID, ActorID, or PID
	Suffix string
}

// TODO(jiangjiawei1103): Can be extracted to a task-specific interface, e.g., TaskSummaryProvider.
type TaskDataResult struct {
	Total                 int              `json:"total"`
	NumAfterTruncation    int              `json:"num_after_truncation"`
	NumFiltered           int              `json:"num_filtered"`
	Result                []map[string]any `json:"result"`
	PartialFailureWarning string           `json:"partial_failure_warning"`
	Warnings              []string         `json:"warnings"`
}

type TaskData struct {
	Result TaskDataResult `json:"result"`
}

type RespTasksInfo struct {
	Result bool     `json:"result"`
	Msg    string   `json:"msg"`
	Data   TaskData `json:"data"`
}

type ReplyActorInfo struct {
	Result bool          `json:"result"`
	Msg    string        `json:"msg"`
	Data   ActorInfoData `json:"data"`
}

type ActorInfoData struct {
	Detail map[string]any `json:"detail"`
}

type ClusterStatusResponse struct {
	Result bool              `json:"result"`
	Msg    string            `json:"msg"`
	Data   ClusterStatusData `json:"data"`
}

type ClusterStatusData struct {
	AutoscalingStatus *string `json:"autoscalingStatus"`
	AutoscalingError  *string `json:"autoscalingError"`
	ClusterStatus     any     `json:"clusterStatus"` // TODO: update it once ray dashboard api /api/cluster_status supports autoscaler info
}

type FormattedClusterStatusResponse struct {
	Result bool                       `json:"result"`
	Msg    string                     `json:"msg"`
	Data   FormattedClusterStatusData `json:"data"`
}

type FormattedClusterStatusData struct {
	ClusterStatus string `json:"clusterStatus"`
}
