package logcollector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	. "github.com/onsi/gomega"
)

const jobsSnapshotKey = "cluster-dir/session_1/fetched_endpoints/restful__api__jobs"

func TestJobsSnapshotCapturesSubmissionsWithoutDrivers(t *testing.T) {
	g := NewWithT(t)
	dash := &fakeDashboard{jobs: `[
		{"type":"SUBMISSION","submission_id":"synthetic_failed","job_id":null,"driver_info":null,"status":"FAILED","entrypoint":"python missing.py","message":"startup failed","error_type":"STARTUP_ERROR","start_time":1700000000123,"end_time":1700000000456,"metadata":{"fixture":"synthetic"},"runtime_env":{"env_vars":{"SYNTHETIC_ENV":"omitted"}},"driver_exit_code":null},
		{"submission_id":"synthetic_pending","job_id":null,"status":"PENDING","runtime_env":"ignored legacy value"},
		{"type":"SUBMISSION","submission_id":"synthetic_stopped","job_id":null,"driver_info":null,"status":"STOPPED"}
	]`}
	srv := dash.start(t)
	handler, writer := newPollTestHandler(t, srv.URL)

	handler.pollAllEndpoints(context.Background(), "session_1", newDatasetPollState(), false)

	g.Expect(dash.requestsFor(jobsEndpoint)).To(HaveLen(1))
	g.Expect(dash.requestsFor(dataDatasetsEndpointPrefix)).To(BeEmpty())
	body, ok := writer.written()[jobsSnapshotKey]
	g.Expect(ok).To(BeTrue(), "submission-only jobs must be captured")
	var records []map[string]any
	g.Expect(json.Unmarshal([]byte(body), &records)).To(Succeed())
	g.Expect(records).To(HaveLen(3))
	for _, record := range records {
		g.Expect(record).To(HaveKeyWithValue("job_id", BeNil()))
		g.Expect(record).To(HaveKeyWithValue("driver_info", BeNil()))
		g.Expect(record).To(HaveKeyWithValue("driver_exit_code", BeNil()))
		g.Expect(record).To(HaveKeyWithValue("runtime_env", BeNil()))
		g.Expect(record).To(HaveKeyWithValue("type", "SUBMISSION"))
	}
	g.Expect(records[0]["status"]).To(Equal("FAILED"))
	g.Expect(records[1]["status"]).To(Equal("PENDING"))
	g.Expect(records[2]["status"]).To(Equal("STOPPED"))
	g.Expect(records[0]["entrypoint"]).To(Equal("python missing.py"))
	g.Expect(records[0]["metadata"]).To(Equal(map[string]any{"fixture": "synthetic"}))
	g.Expect(records[0]["start_time"]).To(Equal(float64(1700000000123)))
	g.Expect(records[0]["end_time"]).To(Equal(float64(1700000000456)))
	g.Expect(records[0]["message"]).To(Equal("startup failed"))
	g.Expect(records[0]["error_type"]).To(Equal("STARTUP_ERROR"))
	g.Expect(body).NotTo(ContainSubstring("SYNTHETIC_ENV"))
}

func TestJobsSnapshotKeepsLastGoodObject(t *testing.T) {
	for _, tc := range []struct {
		name         string
		body         string
		status       int
		writeFailure bool
	}{
		{name: "empty", body: `[]`},
		{name: "null", body: `null`},
		{name: "object", body: `{}`},
		{name: "malformed", body: `[{`},
		{name: "bad row", body: `[{"submission_id":"next","status":"PENDING"},{"job_id":"01000000","status":"INVALID"}]`},
		{name: "wrong field type", body: `[{"submission_id":"next","status":"PENDING","start_time":"invalid"}]`},
		{name: "unauthorized", status: http.StatusUnauthorized},
		{name: "forbidden", status: http.StatusForbidden},
		{name: "http failure", status: http.StatusServiceUnavailable},
		{name: "write failure", body: `[{"submission_id":"next","status":"STOPPED"}]`, writeFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			handler, writer := newPollTestHandler(t, "http://synthetic-dashboard")
			body := `[{"submission_id":"last_good","status":"FAILED"}]`
			status := http.StatusOK
			handler.HttpClient.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if req.URL.Path != jobsEndpoint {
					return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"datasets":[]}`)), Header: make(http.Header), Request: req}, nil
				}
				return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header), Request: req}, nil
			})
			handler.pollDataDatasets(context.Background(), "session_1", newDatasetPollState(), false)
			previous, ok := writer.written()[jobsSnapshotKey]
			g.Expect(ok).To(BeTrue())
			body = tc.body
			if tc.status != 0 {
				status = tc.status
			}
			if tc.writeFailure {
				writer.setWriteErr(fmt.Errorf("synthetic write failure"))
			}
			for _, finalPoll := range []bool{false, true} {
				handler.pollDataDatasets(context.Background(), "session_1", newDatasetPollState(), finalPoll)
				g.Expect(writer.written()[jobsSnapshotKey]).To(Equal(previous))
				g.Expect(writer.order()).To(HaveLen(1), "only the last good snapshot should have been written")
			}
		})
	}
}

func TestJobsSnapshotFailuresDoNotSuppressDatasetDiscovery(t *testing.T) {
	for _, tc := range []struct {
		name         string
		jobs         string
		writeFailure bool
	}{
		{name: "missing identity", jobs: `[{"status":"PENDING"},{"job_id":"01000000","status":"SUCCEEDED"}]`},
		{name: "unknown type", jobs: `[{"type":"UNKNOWN","job_id":"01000000","status":"SUCCEEDED"}]`},
		{name: "mismatched driver", jobs: `[{"job_id":"01000000","status":"SUCCEEDED","driver_info":{"id":"02000000","node_ip_address":"192.0.2.1","pid":"42"}}]`},
		{name: "write failure", jobs: `[{"job_id":"01000000","status":"SUCCEEDED"}]`, writeFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			dash := &fakeDashboard{jobs: tc.jobs, datasets: map[string]string{"01000000": `{"datasets":[{"dataset":"synthetic"}]}`}}
			srv := dash.start(t)
			handler, writer := newPollTestHandler(t, srv.URL)
			if tc.writeFailure {
				handler.Writer = &jobsFailingWriter{MockStorageWriter: writer}
			}
			state := newDatasetPollState()
			handler.pollDataDatasets(context.Background(), "session_1", state, false)

			g.Expect(writer.written()).NotTo(HaveKey(jobsSnapshotKey))
			g.Expect(dash.requestsFor(jobsEndpoint)).To(HaveLen(1))
			g.Expect(dash.requestsFor(dataDatasetsEndpointPrefix)).To(Equal([]string{"/api/data/datasets/01000000"}))
			g.Expect(writer.written()).To(HaveKey("cluster-dir/session_1/fetched_endpoints/restful__api__data__datasets__01000000"))
			g.Expect(state.terminalStored).To(HaveKey("01000000"))
		})
	}
}

type jobsFailingWriter struct {
	*MockStorageWriter
}

func (w *jobsFailingWriter) WriteFile(key string, body io.ReadSeeker) error {
	if key == jobsSnapshotKey {
		return fmt.Errorf("synthetic jobs write failure")
	}
	return w.MockStorageWriter.WriteFile(key, body)
}

func TestJobsSnapshotDoesNotStoreAfterCancellation(t *testing.T) {
	g := NewWithT(t)
	ctx, cancel := context.WithCancel(context.Background())
	handler, writer := newPollTestHandler(t, "http://synthetic-dashboard")
	handler.HttpClient.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK, Header: make(http.Header), Request: req,
			Body: &cancelOnEOFBody{Reader: strings.NewReader(`[{"submission_id":"synthetic","status":"PENDING"}]`), cancel: cancel},
		}, nil
	})
	handler.pollDataDatasets(ctx, "session_1", newDatasetPollState(), false)
	g.Expect(ctx.Err()).To(MatchError(context.Canceled))
	g.Expect(writer.written()).To(BeEmpty())
}

func TestJobsQueryEndpointsRemainConfigured(t *testing.T) {
	g := NewWithT(t)
	dash := &fakeDashboard{jobs: `[]`}
	srv := dash.start(t)
	handler, writer := newPollTestHandler(t, srv.URL)
	handler.AdditionalEndpoints = []string{"/api/jobs?limit=10", "/api/jobs/?limit=10"}
	g.Expect(handler.polledEndpoints()).To(Equal([]string{serveApplicationsEndpoint, placementGroupsEndpoint, "/api/jobs?limit=10", "/api/jobs/?limit=10"}))
	handler.pollAllEndpoints(context.Background(), "session_1", newDatasetPollState(), false)
	g.Expect(dash.requestsFor("/api/jobs")).To(ConsistOf("/api/jobs?limit=10", "/api/jobs/?limit=10", jobsEndpoint))
	g.Expect(writer.written()).To(HaveKey("cluster-dir/session_1/fetched_endpoints/restful__api__jobs?limit=10"))
	g.Expect(writer.written()).NotTo(HaveKey(jobsSnapshotKey))
}

func TestJobsSnapshotAndDatasetWritesRetryIndependently(t *testing.T) {
	g := NewWithT(t)
	dash := &fakeDashboard{
		jobs:     `[{"job_id":"01000000","status":"SUCCEEDED"}]`,
		datasets: map[string]string{"01000000": `{"datasets":[{"dataset":"synthetic"}]}`},
	}
	srv := dash.start(t)
	handler, writer := newPollTestHandler(t, srv.URL)
	state := newDatasetPollState()
	writer.setWriteErr(fmt.Errorf("synthetic storage unavailable"))
	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	g.Expect(writer.written()).To(BeEmpty())
	g.Expect(state.terminalStored).To(BeEmpty())
	g.Expect(state.emptyRuns).To(BeEmpty())

	writer.setWriteErr(nil)
	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	g.Expect(writer.written()).To(HaveKey(jobsSnapshotKey))
	g.Expect(writer.written()).To(HaveKey("cluster-dir/session_1/fetched_endpoints/restful__api__data__datasets__01000000"))
	g.Expect(state.terminalStored).To(HaveKey("01000000"))

	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	g.Expect(dash.requestsFor(jobsEndpoint)).To(HaveLen(3), "snapshot must keep refreshing after dataset settles")
	g.Expect(dash.requestsFor(dataDatasetsEndpointPrefix)).To(HaveLen(2), "settled terminal dataset must not be fetched again")
}

func TestJobsSnapshotFetchUsesAuthentication(t *testing.T) {
	g := NewWithT(t)
	t.Setenv("RAY_AUTH_TOKEN", "synthetic-test-credential")
	t.Setenv("RAY_ENABLE_K8S_TOKEN_AUTH", "")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("x-ray-authorization") != "Bearer synthetic-test-credential" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`[{"submission_id":"synthetic","status":"PENDING"}]`))
	}))
	t.Cleanup(srv.Close)
	handler, writer := newPollTestHandler(t, srv.URL)
	handler.pollDataDatasets(context.Background(), "session_1", newDatasetPollState(), false)
	g.Expect(writer.written()).To(HaveKey(jobsSnapshotKey))
}
