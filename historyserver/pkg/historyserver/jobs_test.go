package historyserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const (
	jobLookupClusterName = "cluster"
	jobLookupNamespace   = "default"
	jobLookupSessionName = "session"
)

var jobLookupSessionKey = utils.BuildClusterSessionKey(jobLookupClusterName, jobLookupNamespace, jobLookupSessionName)

func newJobLookupHandler(t *testing.T, jobs map[string]eventtypes.Job) *ServerHandler {
	t.Helper()
	loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{cacheSize: 1})
	require.NoError(t, loader.putSnapshot(jobLookupSessionKey, &eventserver.SessionSnapshot{
		SessionKey: jobLookupSessionKey,
		Jobs:       jobs,
	}, utils.ClusterInfo{}, false))
	return &ServerHandler{sessionLoader: loader}
}

func serveJobDetail(t *testing.T, handler *ServerHandler, id string) *httptest.ResponseRecorder {
	t.Helper()
	container := restful.NewContainer()
	ws := new(restful.WebService)
	ws.Path("/api").Produces(restful.MIME_JSON)
	ws.Route(ws.GET("/jobs/{job_id}").To(handler.getJob).Filter(handler.CookieHandle))
	container.Add(ws)
	request := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/api/jobs/"+id, nil)
	for key, value := range map[string]string{
		COOKIE_CLUSTER_NAME_KEY:      jobLookupClusterName,
		COOKIE_CLUSTER_NAMESPACE_KEY: jobLookupNamespace,
		COOKIE_SESSION_NAME_KEY:      jobLookupSessionName,
	} {
		request.AddCookie(&http.Cookie{Name: key, Value: value})
	}
	response := httptest.NewRecorder()
	container.ServeHTTP(response, request)
	return response
}

func decodeJob(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var job map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	return job
}

func TestFindJobByID(t *testing.T) {
	// One snapshot covers every lookup path: explicit and metadata submission
	// IDs, multiple drivers for one submission, and unrelated jobs.
	jobs := map[string]eventtypes.Job{
		"01000000": {
			JobID: "01000000", SubmissionID: "raysubmit-explicit", Status: eventtypes.JOBFAILED,
			Config: eventtypes.DriverJobConfig{Metadata: map[string]string{"job_submission_id": "raysubmit-stale"}},
		},
		"02000000": {JobID: "02000000", Status: eventtypes.JOBRUNNING},
		"03000000": {
			JobID: "03000000", Status: eventtypes.SUCCEEDED,
			Config: eventtypes.DriverJobConfig{Metadata: map[string]string{"job_submission_id": "raysubmit-metadata"}},
		},
		"04000000": {JobID: "04000000", SubmissionID: "raysubmit-multi", Status: eventtypes.JOBFAILED},
		"05000000": {
			JobID: "05000000", Status: eventtypes.SUCCEEDED,
			Config: eventtypes.DriverJobConfig{Metadata: map[string]string{"job_submission_id": "raysubmit-multi"}},
		},
		"06000000": {JobID: "06000000", SubmissionID: "02000000", Status: eventtypes.JOBFAILED},
	}
	tests := []struct {
		name      string
		id        string
		wantJobID string
		wantFound bool
	}{
		{"core id", "03000000", "03000000", true},
		{"core id takes precedence over submission id", "02000000", "02000000", true},
		{"submission id from explicit field", "raysubmit-explicit", "01000000", true},
		{"submission id from metadata", "raysubmit-metadata", "03000000", true},
		{"stale metadata is not resolvable", "raysubmit-stale", "", false},
		{"multiple drivers use last core id", "raysubmit-multi", "05000000", true},
		{"no submission id does not match", "raysubmit-unknown", "", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job, found := findJobByID(jobs, test.id)
			require.Equal(t, test.wantFound, found)
			if !test.wantFound {
				require.Equal(t, eventtypes.Job{}, job)
				return
			}
			require.Equal(t, jobs[test.wantJobID], job)
		})
	}

	t.Run("nil map", func(t *testing.T) {
		job, found := findJobByID(nil, "raysubmit-explicit")
		require.False(t, found)
		require.Equal(t, eventtypes.Job{}, job)
	})

	t.Run("empty id", func(t *testing.T) {
		// A direct map lookup would find the empty key; the guard must not.
		job, found := findJobByID(map[string]eventtypes.Job{"": {}}, "")
		require.False(t, found)
		require.Equal(t, eventtypes.Job{}, job)
	})
}

func TestGetJobHTTP(t *testing.T) {
	const submissionID = "raysubmit-metadata"
	handler := newJobLookupHandler(t, map[string]eventtypes.Job{
		"03000000": {
			JobID: "03000000", Status: eventtypes.SUCCEEDED, StartTime: time.UnixMilli(2000),
			Config: eventtypes.DriverJobConfig{Metadata: map[string]string{"job_submission_id": submissionID}},
		},
	})

	t.Run("core job id returns 200", func(t *testing.T) {
		response := serveJobDetail(t, handler, "03000000")
		require.Equal(t, http.StatusOK, response.Code)
		job := decodeJob(t, response)
		require.Equal(t, "03000000", job["job_id"])
		require.Equal(t, submissionID, job["submission_id"])
		require.Equal(t, "SUCCEEDED", job["status"])
		require.InDelta(t, 2000, job["start_time"], 0)
	})

	t.Run("submission id returns the same job", func(t *testing.T) {
		core := serveJobDetail(t, handler, "03000000")
		submission := serveJobDetail(t, handler, submissionID)
		require.Equal(t, http.StatusOK, submission.Code)
		require.JSONEq(t, core.Body.String(), submission.Body.String())
	})

	t.Run("unknown job id returns 404", func(t *testing.T) {
		response := serveJobDetail(t, handler, "raysubmit-unknown")
		require.Equal(t, http.StatusNotFound, response.Code)
		require.Equal(t, "Job raysubmit-unknown does not exist", response.Body.String())
	})

	t.Run("missing snapshot returns 503", func(t *testing.T) {
		loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{cacheSize: 1})
		response := serveJobDetail(t, &ServerHandler{sessionLoader: loader}, "03000000")
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
	})
}

func TestGetJobDoesNotMutateSnapshot(t *testing.T) {
	handler := newJobLookupHandler(t, map[string]eventtypes.Job{
		"02000000": {
			JobID: "02000000", Status: eventtypes.JOBFAILED,
			Config: eventtypes.DriverJobConfig{Metadata: map[string]string{"job_submission_id": "raysubmit-metadata"}},
		},
	})
	snapshot, found := handler.sessionLoader.GetSnapshot(jobLookupSessionKey)
	require.True(t, found)
	before, err := json.Marshal(snapshot)
	require.NoError(t, err)
	response := serveJobDetail(t, handler, "raysubmit-metadata")
	require.Equal(t, http.StatusOK, response.Code)
	require.Equal(t, "raysubmit-metadata", decodeJob(t, response)["submission_id"])
	after, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
