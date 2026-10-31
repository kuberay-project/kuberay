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
)

const jobLookupSessionKey = "cluster_default_session"

func newJobLookupHandler(t *testing.T, jobs map[string]eventtypes.Job) *ServerHandler {
	t.Helper()
	loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{cacheSize: 1})
	require.NoError(t, loader.putSnapshot(jobLookupSessionKey, &eventserver.SessionSnapshot{
		SessionKey: jobLookupSessionKey,
		Jobs:       jobs,
	}))
	return &ServerHandler{sessionLoader: loader}
}

func serveJobDetail(handler *ServerHandler, id string) *httptest.ResponseRecorder {
	container := restful.NewContainer()
	ws := new(restful.WebService)
	ws.Path("/api").Produces(restful.MIME_JSON)
	ws.Route(ws.GET("/jobs/{job_id}").To(handler.getJob).Filter(handler.CookieHandle))
	container.Add(ws)
	request := httptest.NewRequest("GET", "/api/jobs/"+id, nil)
	for key, value := range map[string]string{
		COOKIE_CLUSTER_NAME_KEY:      "cluster",
		COOKIE_CLUSTER_NAMESPACE_KEY: "default",
		COOKIE_SESSION_NAME_KEY:      "session",
	} {
		request.AddCookie(&http.Cookie{Name: key, Value: value})
	}
	response := httptest.NewRecorder()
	container.ServeHTTP(response, request)
	return response
}

func TestGetJobByCoreOrSubmissionID(t *testing.T) {
	handler := newJobLookupHandler(t, map[string]eventtypes.Job{
		"02000000": {JobID: "02000000", SubmissionID: "raysubmit-example", Status: eventtypes.JOBFAILED},
	})
	core := serveJobDetail(handler, "02000000")
	require.Equal(t, http.StatusOK, core.Code)
	var job map[string]any
	require.NoError(t, json.Unmarshal(core.Body.Bytes(), &job))
	require.Equal(t, "02000000", job["job_id"])
	require.Equal(t, "raysubmit-example", job["submission_id"])
	require.Equal(t, "FAILED", job["status"])

	submission := serveJobDetail(handler, "raysubmit-example")
	require.Equal(t, http.StatusOK, submission.Code)
	require.JSONEq(t, core.Body.String(), submission.Body.String())
}

func TestGetJobByMetadataSubmissionID(t *testing.T) {
	handler := newJobLookupHandler(t, map[string]eventtypes.Job{
		"03000000": {
			JobID: "03000000", Status: eventtypes.SUCCEEDED,
			Config: eventtypes.DriverJobConfig{Metadata: map[string]string{"job_submission_id": "raysubmit-metadata"}},
		},
	})
	core := serveJobDetail(handler, "03000000")
	submission := serveJobDetail(handler, "raysubmit-metadata")
	require.Equal(t, http.StatusOK, submission.Code)
	require.JSONEq(t, core.Body.String(), submission.Body.String())
}

func TestGetJobCoreIDTakesPrecedence(t *testing.T) {
	handler := newJobLookupHandler(t, map[string]eventtypes.Job{
		"02000000": {JobID: "02000000", SubmissionID: "raysubmit-core", Status: eventtypes.SUCCEEDED},
		"03000000": {JobID: "03000000", SubmissionID: "02000000", Status: eventtypes.JOBFAILED},
	})
	response := serveJobDetail(handler, "02000000")
	var job map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	require.Equal(t, "02000000", job["job_id"])
	require.Equal(t, "SUCCEEDED", job["status"])
}

func TestGetJobSubmissionUsesLastDriver(t *testing.T) {
	handler := newJobLookupHandler(t, map[string]eventtypes.Job{
		"02000000": {
			JobID: "02000000", SubmissionID: "raysubmit-multi", Status: eventtypes.JOBFAILED,
			StartTime: time.Unix(2, 0),
		},
		"03000000": {
			JobID: "03000000", Status: eventtypes.SUCCEEDED, StartTime: time.Unix(1, 0),
			Config: eventtypes.DriverJobConfig{Metadata: map[string]string{"job_submission_id": "raysubmit-multi"}},
		},
		"04000000": {JobID: "04000000", Status: eventtypes.JOBRUNNING},
	})
	response := serveJobDetail(handler, "raysubmit-multi")
	var job map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	require.Equal(t, "03000000", job["job_id"])
	require.Equal(t, "SUCCEEDED", job["status"])
	require.Equal(t, "raysubmit-multi", job["submission_id"])
}

func TestGetJobExplicitSubmissionIDTakesPrecedence(t *testing.T) {
	handler := newJobLookupHandler(t, map[string]eventtypes.Job{
		"02000000": {
			JobID: "02000000", SubmissionID: "raysubmit-explicit", Status: eventtypes.JOBFAILED,
			Config: eventtypes.DriverJobConfig{Metadata: map[string]string{"job_submission_id": "raysubmit-stale"}},
		},
	})
	core := serveJobDetail(handler, "02000000")
	submission := serveJobDetail(handler, "raysubmit-explicit")
	require.JSONEq(t, core.Body.String(), submission.Body.String())
	stale := serveJobDetail(handler, "raysubmit-stale")
	require.Equal(t, http.StatusOK, stale.Code)
	require.Equal(t, "Job raysubmit-stale does not exist", stale.Body.String())
}

func TestGetJobMissingDataBehavior(t *testing.T) {
	handler := newJobLookupHandler(t, nil)
	unknown := serveJobDetail(handler, "unknown")
	require.Equal(t, http.StatusOK, unknown.Code)
	require.Equal(t, "Job unknown does not exist", unknown.Body.String())
	loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{cacheSize: 1})
	missing := serveJobDetail(&ServerHandler{sessionLoader: loader}, "02000000")
	require.Equal(t, http.StatusServiceUnavailable, missing.Code)
	require.Contains(t, missing.Body.String(), "session snapshot not in cache")
}

func TestFindJobByIDMissingIDs(t *testing.T) {
	tests := []struct {
		name string
		jobs map[string]eventtypes.Job
		id   string
	}{
		{name: "nil map", id: "unknown"},
		{name: "no submission ID", jobs: map[string]eventtypes.Job{"02000000": {JobID: "02000000"}}, id: "unknown"},
		{name: "empty ID", jobs: map[string]eventtypes.Job{"": {}, "02000000": {JobID: "02000000"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			job, found := findJobByID(test.jobs, test.id)
			require.False(t, found)
			require.Equal(t, eventtypes.Job{}, job)
		})
	}
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
	response := serveJobDetail(handler, "raysubmit-metadata")
	require.Equal(t, http.StatusOK, response.Code)
	var job map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	require.Equal(t, "raysubmit-metadata", job["submission_id"])
	after, err := json.Marshal(snapshot)
	require.NoError(t, err)
	require.Equal(t, before, after)
}
