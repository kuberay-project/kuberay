package logcollector

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const trainRunsEndpoint = "/api/train/v2/runs/v1"

// fakeDashboard stands in for the Ray Dashboard, recording every requested path.
type fakeDashboard struct {
	mu       sync.Mutex
	requests []string
	// jobs is the /api/jobs/ response body.
	jobs string
	// datasets maps a job ID to its /api/data/datasets/{job_id} response body.
	datasets map[string]string
}

func (f *fakeDashboard) start(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.requests = append(f.requests, r.URL.RequestURI())
		f.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == jobsEndpoint:
			_, _ = w.Write([]byte(f.jobs))
		case r.URL.Path == serveApplicationsEndpoint:
			// Non-empty by default: an empty Serve response is deliberately not stored.
			_, _ = w.Write([]byte(`{"applications": {"app": {"status": "RUNNING"}}}`))
		case strings.HasPrefix(r.URL.Path, dataDatasetsEndpointPrefix):
			jobID := strings.TrimPrefix(r.URL.Path, dataDatasetsEndpointPrefix)
			body, ok := f.datasets[jobID]
			if !ok {
				body = `{"datasets": []}`
			}
			_, _ = w.Write([]byte(body))
		default:
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (f *fakeDashboard) requestsFor(prefix string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []string
	for _, req := range f.requests {
		if strings.HasPrefix(req, prefix) {
			out = append(out, req)
		}
	}
	return out
}

func newPollTestHandler(t *testing.T, dashboardAddr string) (*RayLogHandler, *MockStorageWriter) {
	t.Helper()
	writer := NewMockStorageWriter()
	return &RayLogHandler{
		Writer:           writer,
		HttpClient:       &http.Client{},
		ShutdownChan:     make(chan struct{}),
		ClusterDir:       "cluster-dir",
		DashboardAddress: dashboardAddr,
		IsHead:           true,
	}, writer
}

func writtenKeys(writer *MockStorageWriter) []string {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	keys := make([]string, 0, len(writer.writtenFiles))
	for k := range writer.writtenFiles {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

type blockingStorageWriter struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingStorageWriter) CreateDirectory(string) error {
	return nil
}

func (w *blockingStorageWriter) WriteFile(string, io.ReadSeeker) error {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return nil
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type cancelOnEOFBody struct {
	io.Reader
	cancel context.CancelFunc
}

func (b *cancelOnEOFBody) Read(p []byte) (int, error) {
	n, err := b.Reader.Read(p)
	if err == io.EOF {
		b.cancel()
	}
	return n, err
}

func (b *cancelOnEOFBody) Close() error {
	return nil
}

// TestPollDataDatasetsFansOutPerJob verifies per-job fan-out from /api/jobs/, skipping blank IDs.
func TestPollDataDatasetsFansOutPerJob(t *testing.T) {
	g := NewWithT(t)

	dash := &fakeDashboard{
		jobs: `[
			{"job_id": "01000000", "status": "RUNNING"},
			{"job_id": null, "status": "PENDING"},
			{"job_id": "02000000", "status": "RUNNING"}
		]`,
		datasets: map[string]string{
			"01000000": `{"datasets": [{"dataset": "ds_a", "job_id": "01000000"}]}`,
			"02000000": `{"datasets": [{"dataset": "ds_b", "job_id": "02000000"}]}`,
		},
	}
	srv := dash.start(t)
	handler, writer := newPollTestHandler(t, srv.URL)

	handler.pollDataDatasets(context.Background(), "session_1", newDatasetPollState(), false)

	// The blank job_id is skipped.
	g.Expect(dash.requestsFor(dataDatasetsEndpointPrefix)).To(ConsistOf(
		"/api/data/datasets/01000000",
		"/api/data/datasets/02000000",
	))
	g.Expect(writtenKeys(writer)).To(Equal([]string{
		"cluster-dir/session_1/fetched_endpoints/restful__api__data__datasets__01000000",
		"cluster-dir/session_1/fetched_endpoints/restful__api__data__datasets__02000000",
	}))
}

// TestPollDataDatasetsSkipsEmptyResponse verifies an empty datasets response is never written,
// so a stats-actor eviction cannot replace datasets captured earlier.
func TestPollDataDatasetsSkipsEmptyResponse(t *testing.T) {
	g := NewWithT(t)

	dash := &fakeDashboard{
		jobs: `[{"job_id": "01000000", "status": "RUNNING"}]`,
		// No entry, so the fake dashboard replies {"datasets": []}.
		datasets: map[string]string{},
	}
	srv := dash.start(t)
	handler, writer := newPollTestHandler(t, srv.URL)

	handler.pollDataDatasets(context.Background(), "session_1", newDatasetPollState(), false)

	g.Expect(dash.requestsFor(dataDatasetsEndpointPrefix)).To(HaveLen(1))
	g.Expect(writtenKeys(writer)).To(BeEmpty())
}

// TestPollDataDatasetsDoesNotCountRunningEmpties verifies the retry cap begins only after
// a job becomes terminal, so early empty responses cannot suppress its first terminal fetch.
func TestPollDataDatasetsDoesNotCountRunningEmpties(t *testing.T) {
	g := NewWithT(t)

	var mu sync.Mutex
	status := "RUNNING"
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == jobsEndpoint {
			_, _ = fmt.Fprintf(w, `[{"job_id": "01000000", "status": %q}]`, status)
			return
		}
		attempts++
		if status == "RUNNING" {
			_, _ = w.Write([]byte(`{"datasets": []}`))
			return
		}
		_, _ = w.Write([]byte(`{"datasets": [{"dataset": "complete"}]}`))
	}))
	t.Cleanup(srv.Close)

	handler, writer := newPollTestHandler(t, srv.URL)
	state := newDatasetPollState()
	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	g.Expect(state.emptyRuns).NotTo(HaveKey("01000000"))

	mu.Lock()
	status = "SUCCEEDED"
	mu.Unlock()
	handler.pollDataDatasets(context.Background(), "session_1", state, false)

	mu.Lock()
	defer mu.Unlock()
	g.Expect(attempts).To(Equal(3))
	g.Expect(state.terminalStored).To(HaveKey("01000000"))
	g.Expect(writtenKeys(writer)).To(HaveLen(1))
}

// TestPollDataDatasetsStopsPollingTerminalJobs verifies a terminal job is fetched once
// while a running job keeps being refreshed.
func TestPollDataDatasetsStopsPollingTerminalJobs(t *testing.T) {
	g := NewWithT(t)

	dash := &fakeDashboard{
		jobs: `[
			{"job_id": "01000000", "status": "SUCCEEDED"},
			{"job_id": "02000000", "status": "RUNNING"}
		]`,
		datasets: map[string]string{
			"01000000": `{"datasets": [{"dataset": "ds_done"}]}`,
			"02000000": `{"datasets": [{"dataset": "ds_live"}]}`,
		},
	}
	srv := dash.start(t)
	handler, _ := newPollTestHandler(t, srv.URL)

	state := newDatasetPollState()
	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	handler.pollDataDatasets(context.Background(), "session_1", state, false)

	g.Expect(state.terminalStored).To(HaveKey("01000000"))
	g.Expect(state.terminalStored).NotTo(HaveKey("02000000"))
	// The terminal job is fetched only on the first cycle; the running one every cycle.
	g.Expect(dash.requestsFor("/api/data/datasets/01000000")).To(HaveLen(1))
	g.Expect(dash.requestsFor("/api/data/datasets/02000000")).To(HaveLen(3))
}

// TestPollDataDatasetsRetriesFailedTerminalJob verifies a failed fetch is retried, so a
// transient dashboard error does not lose a terminal job's datasets.
func TestPollDataDatasetsRetriesFailedTerminalJob(t *testing.T) {
	g := NewWithT(t)

	var mu sync.Mutex
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == jobsEndpoint {
			_, _ = w.Write([]byte(`[{"job_id": "01000000", "status": "SUCCEEDED"}]`))
			return
		}
		mu.Lock()
		attempts++
		first := attempts == 1
		mu.Unlock()
		if first {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(`{"datasets": [{"dataset": "ds_done"}]}`))
	}))
	t.Cleanup(srv.Close)

	handler, writer := newPollTestHandler(t, srv.URL)

	state := newDatasetPollState()
	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	g.Expect(state.terminalStored).To(BeEmpty())
	g.Expect(writtenKeys(writer)).To(BeEmpty())

	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	g.Expect(state.terminalStored).To(HaveKey("01000000"))
	g.Expect(writtenKeys(writer)).To(Equal([]string{
		"cluster-dir/session_1/fetched_endpoints/restful__api__data__datasets__01000000",
	}))
}

// TestPollDataDatasetsRetriesTerminalJobWithLateStats verifies an empty first response is
// polled again, because Ray Data registers stats slightly after the job reports SUCCEEDED.
func TestPollDataDatasetsRetriesTerminalJobWithLateStats(t *testing.T) {
	g := NewWithT(t)

	var mu sync.Mutex
	attempts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == jobsEndpoint {
			_, _ = w.Write([]byte(`[{"job_id": "01000000", "status": "SUCCEEDED"}]`))
			return
		}
		mu.Lock()
		attempts++
		first := attempts == 1
		mu.Unlock()
		if first {
			_, _ = w.Write([]byte(`{"datasets": []}`))
			return
		}
		_, _ = w.Write([]byte(`{"datasets": [{"dataset": "ds_late"}]}`))
	}))
	t.Cleanup(srv.Close)

	handler, writer := newPollTestHandler(t, srv.URL)

	state := newDatasetPollState()
	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	g.Expect(state.terminalStored).To(BeEmpty())
	g.Expect(writtenKeys(writer)).To(BeEmpty())

	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	g.Expect(state.terminalStored).To(HaveKey("01000000"))
	g.Expect(writtenKeys(writer)).To(Equal([]string{
		"cluster-dir/session_1/fetched_endpoints/restful__api__data__datasets__01000000",
	}))
}

// TestPollDataDatasetsGivesUpOnRepeatedlyEmptyTerminalJob verifies the retry is bounded.
func TestPollDataDatasetsGivesUpOnRepeatedlyEmptyTerminalJob(t *testing.T) {
	g := NewWithT(t)

	dash := &fakeDashboard{
		jobs: `[{"job_id": "01000000", "status": "SUCCEEDED"}]`,
		// No entry, so the fake dashboard always replies {"datasets": []}.
		datasets: map[string]string{},
	}
	srv := dash.start(t)
	handler, writer := newPollTestHandler(t, srv.URL)

	state := newDatasetPollState()
	for range 4 {
		handler.pollDataDatasets(context.Background(), "session_1", state, false)
	}

	g.Expect(state.terminalStored).NotTo(HaveKey("01000000"))
	g.Expect(state.emptyRuns).To(HaveKeyWithValue("01000000", terminalEmptyPollsBeforeGivingUp))
	g.Expect(dash.requestsFor("/api/data/datasets/01000000")).To(HaveLen(terminalEmptyPollsBeforeGivingUp))
	g.Expect(writtenKeys(writer)).To(BeEmpty())
}

// TestFinalDatasetPollReusesPeriodicState verifies the final poll skips terminal jobs
// already stored, while retrying terminal jobs that were empty or failed periodically.
func TestFinalDatasetPollReusesPeriodicState(t *testing.T) {
	g := NewWithT(t)

	var mu sync.Mutex
	attempts := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == jobsEndpoint {
			_, _ = w.Write([]byte(`[
				{"job_id": "01000000", "status": "SUCCEEDED"},
				{"job_id": "02000000", "status": "SUCCEEDED"},
				{"job_id": "03000000", "status": "SUCCEEDED"}
			]`))
			return
		}
		jobID := strings.TrimPrefix(r.URL.Path, dataDatasetsEndpointPrefix)
		mu.Lock()
		attempts[jobID]++
		attempt := attempts[jobID]
		mu.Unlock()

		switch jobID {
		case "01000000":
			_, _ = w.Write([]byte(`{"datasets": [{"dataset": "stored"}]}`))
		case "02000000":
			if attempt <= terminalEmptyPollsBeforeGivingUp {
				_, _ = w.Write([]byte(`{"datasets": []}`))
				return
			}
			_, _ = w.Write([]byte(`{"datasets": [{"dataset": "late"}]}`))
		case "03000000":
			if attempt <= 2 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte(`{"datasets": [{"dataset": "retried"}]}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	handler, writer := newPollTestHandler(t, srv.URL)
	state := newDatasetPollState()
	handler.pollDataDatasets(context.Background(), "session_1", state, false)
	handler.pollDataDatasets(context.Background(), "session_1", state, false)

	g.Expect(state.terminalStored).To(HaveKey("01000000"))
	g.Expect(state.terminalStored).NotTo(HaveKey("02000000"))
	g.Expect(state.terminalStored).NotTo(HaveKey("03000000"))
	g.Expect(state.emptyRuns).To(HaveKeyWithValue("02000000", terminalEmptyPollsBeforeGivingUp))

	handler.pollDataDatasets(context.Background(), "session_1", state, true)

	mu.Lock()
	defer mu.Unlock()
	g.Expect(attempts).To(Equal(map[string]int{
		"01000000": 1,
		"02000000": 3,
		"03000000": 3,
	}))
	g.Expect(state.terminalStored).To(HaveKey("01000000"))
	g.Expect(state.terminalStored).To(HaveKey("02000000"))
	g.Expect(state.terminalStored).To(HaveKey("03000000"))
	g.Expect(state.emptyRuns).NotTo(HaveKey("02000000"))
	g.Expect(writtenKeys(writer)).To(ConsistOf(
		"cluster-dir/session_1/fetched_endpoints/restful__api__data__datasets__01000000",
		"cluster-dir/session_1/fetched_endpoints/restful__api__data__datasets__02000000",
		"cluster-dir/session_1/fetched_endpoints/restful__api__data__datasets__03000000",
	))
}

// TestPollAllEndpointsStoresDefaultEndpoints verifies the built-in endpoints are stored under
// the exact URIs the frontend requests.
func TestPollAllEndpointsStoresDefaultEndpoints(t *testing.T) {
	g := NewWithT(t)

	dash := &fakeDashboard{jobs: `[]`}
	srv := dash.start(t)
	handler, writer := newPollTestHandler(t, srv.URL)

	handler.pollAllEndpoints(context.Background(), "session_1", newDatasetPollState(), false)

	g.Expect(dash.requestsFor("/api/serve/applications/")).To(HaveLen(1))
	g.Expect(dash.requestsFor("/api/v0/placement_groups?detail=1&limit=10000")).To(HaveLen(1))
	g.Expect(writtenKeys(writer)).To(Equal([]string{
		"cluster-dir/session_1/fetched_endpoints/restful__api__serve__applications",
		"cluster-dir/session_1/fetched_endpoints/restful__api__v0__placement_groups?detail=1&limit=10000",
	}))
}

// TestPollCycleFollowsSessionChange verifies a Ray head restart moves polling to the new
// session and resets state, since job IDs restart with the session.
func TestPollCycleFollowsSessionChange(t *testing.T) {
	g := NewWithT(t)

	tmpRoot := t.TempDir()
	t.Setenv("RAY_TMP_ROOT", tmpRoot)
	symlink := filepath.Join(tmpRoot, "session_latest")
	pointAt := func(session string) {
		g.Expect(os.MkdirAll(filepath.Join(tmpRoot, session), 0o755)).To(Succeed())
		_ = os.Remove(symlink)
		g.Expect(os.Symlink(filepath.Join(tmpRoot, session), symlink)).To(Succeed())
	}

	dash := &fakeDashboard{
		jobs:     `[{"job_id": "01000000", "status": "SUCCEEDED"}]`,
		datasets: map[string]string{"01000000": `{"datasets": [{"dataset": "ds"}]}`},
	}
	srv := dash.start(t)
	handler, writer := newPollTestHandler(t, srv.URL)

	pointAt("session_old")
	session, state := handler.pollCycle(context.Background(), "session_old", newDatasetPollState())
	g.Expect(session).To(Equal("session_old"))
	g.Expect(state.terminalStored).To(HaveKey("01000000"))

	pointAt("session_new")
	session, state = handler.pollCycle(context.Background(), session, state)
	g.Expect(session).To(Equal("session_new"))
	g.Expect(state.terminalStored).To(HaveKey("01000000"), "the new session's job must be captured, not skipped")

	// The same job ID is stored once per session, not once overall.
	g.Expect(dash.requestsFor("/api/data/datasets/01000000")).To(HaveLen(2))
	g.Expect(writtenKeys(writer)).To(ContainElements(
		"cluster-dir/session_old/fetched_endpoints/restful__api__data__datasets__01000000",
		"cluster-dir/session_new/fetched_endpoints/restful__api__data__datasets__01000000",
	))
}

// TestFinalPollReusesStateOnlyForCurrentSession verifies a restarted Ray head cannot
// inherit terminal job IDs from the previous session.
func TestFinalPollReusesStateOnlyForCurrentSession(t *testing.T) {
	for _, tc := range []struct {
		name            string
		previousSession string
		wantRequests    int
	}{
		{name: "same session", previousSession: "session_new", wantRequests: 0},
		{name: "changed session", previousSession: "session_old", wantRequests: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			tmpRoot := t.TempDir()
			t.Setenv("RAY_TMP_ROOT", tmpRoot)
			g.Expect(os.MkdirAll(filepath.Join(tmpRoot, "session_new"), 0o755)).To(Succeed())
			g.Expect(os.Symlink(filepath.Join(tmpRoot, "session_new"), filepath.Join(tmpRoot, "session_latest"))).To(Succeed())

			dash := &fakeDashboard{
				jobs:     `[{"job_id": "01000000", "status": "SUCCEEDED"}]`,
				datasets: map[string]string{"01000000": `{"datasets": [{"dataset": "new"}]}`},
			}
			srv := dash.start(t)
			handler, writer := newPollTestHandler(t, srv.URL)
			state := newDatasetPollState()
			state.terminalStored["01000000"] = struct{}{}

			handler.processAdditionalEndpoints(periodicPollResult{
				sessionName: tc.previousSession,
				state:       state,
			})

			g.Expect(dash.requestsFor("/api/data/datasets/01000000")).To(HaveLen(tc.wantRequests))
			dataKey := "cluster-dir/session_new/fetched_endpoints/restful__api__data__datasets__01000000"
			if tc.wantRequests == 0 {
				g.Expect(writtenKeys(writer)).NotTo(ContainElement(dataKey))
			} else {
				g.Expect(writtenKeys(writer)).To(ContainElement(dataKey))
			}
		})
	}
}

// TestPeriodicPollingStopsOnShutdownSignal verifies the loop exits on the shutdown signal,
// not ShutdownChan, so no tick can overwrite the final shutdown snapshot.
func TestPeriodicPollingStopsOnShutdownSignal(t *testing.T) {
	g := NewWithT(t)

	tmpRoot := t.TempDir()
	t.Setenv("RAY_TMP_ROOT", tmpRoot)
	g.Expect(os.MkdirAll(filepath.Join(tmpRoot, "session_1"), 0o755)).To(Succeed())
	g.Expect(os.Symlink(filepath.Join(tmpRoot, "session_1"), filepath.Join(tmpRoot, "session_latest"))).To(Succeed())

	dash := &fakeDashboard{jobs: `[]`}
	srv := dash.start(t)
	handler, _ := newPollTestHandler(t, srv.URL)
	handler.EndpointPollInterval = time.Hour

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		handler.pollAdditionalEndpointsPeriodically(stop)
		close(done)
	}()

	// ShutdownChan stays open; the stop signal alone must end the loop.
	close(stop)
	g.Eventually(done).Should(BeClosed())
}

// TestPeriodicPollingCancelsInFlightRequestOnShutdown verifies the stop signal aborts a
// cycle mid-request: Run joins this goroutine before the final poll, so a blocked request
// must neither stall shutdown nor store anything after being canceled.
func TestPeriodicPollingCancelsInFlightRequestOnShutdown(t *testing.T) {
	g := NewWithT(t)

	tmpRoot := t.TempDir()
	t.Setenv("RAY_TMP_ROOT", tmpRoot)
	g.Expect(os.MkdirAll(filepath.Join(tmpRoot, "session_1"), 0o755)).To(Succeed())
	g.Expect(os.Symlink(filepath.Join(tmpRoot, "session_1"), filepath.Join(tmpRoot, "session_latest"))).To(Succeed())

	var entered sync.Once
	enteredCh := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		entered.Do(func() { close(enteredCh) })
		<-release
	}))
	t.Cleanup(srv.Close)

	handler, writer := newPollTestHandler(t, srv.URL)
	handler.EndpointPollInterval = time.Hour

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		handler.pollAdditionalEndpointsPeriodically(stop)
		close(done)
	}()

	g.Eventually(enteredCh).Should(BeClosed())
	close(stop)
	g.Eventually(done, "5s").Should(BeClosed())
	g.Expect(writtenKeys(writer)).To(BeEmpty())
}

// TestPeriodicPollTimeoutBuffersLateResult verifies a timed-out join does not share state
// and the periodic goroutine can still transfer its result and exit after a blocked write.
func TestPeriodicPollTimeoutBuffersLateResult(t *testing.T) {
	g := NewWithT(t)

	tmpRoot := t.TempDir()
	t.Setenv("RAY_TMP_ROOT", tmpRoot)
	g.Expect(os.MkdirAll(filepath.Join(tmpRoot, "session_1"), 0o755)).To(Succeed())
	g.Expect(os.Symlink(filepath.Join(tmpRoot, "session_1"), filepath.Join(tmpRoot, "session_latest"))).To(Succeed())

	dash := &fakeDashboard{jobs: `[]`}
	srv := dash.start(t)
	writer := &blockingStorageWriter{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	handler, _ := newPollTestHandler(t, srv.URL)
	handler.Writer = writer
	handler.EndpointPollInterval = time.Hour

	stop := make(chan struct{})
	results := handler.startPeriodicEndpointPolling(stop)
	g.Eventually(writer.entered).Should(BeClosed())
	close(stop)

	result, joined := waitForPeriodicPollResult(results, 0)
	g.Expect(joined).To(BeFalse())
	g.Expect(result.state).To(BeNil())

	close(writer.release)
	g.Eventually(func() int { return len(results) }).Should(Equal(1))
	lateResult := <-results
	g.Expect(lateResult.sessionName).To(Equal("session_1"))
	g.Expect(lateResult.state).NotTo(BeNil())
}

// TestPollAllEndpointsStopsWhenContextExpires verifies a canceled context prevents new requests.
func TestPollAllEndpointsStopsWhenContextExpires(t *testing.T) {
	g := NewWithT(t)

	dash := &fakeDashboard{jobs: `[{"job_id": "01000000", "status": "SUCCEEDED"}]`}
	srv := dash.start(t)
	handler, writer := newPollTestHandler(t, srv.URL)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	handler.pollAllEndpoints(ctx, "session_1", newDatasetPollState(), false)

	g.Expect(dash.requestsFor("/")).To(BeEmpty())
	g.Expect(writtenKeys(writer)).To(BeEmpty())
}

// TestPollSingleEndpointDoesNotStoreAfterCancellation verifies a response fetched just as
// shutdown starts remains retryable instead of being recorded as durably stored.
func TestPollSingleEndpointDoesNotStoreAfterCancellation(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "non-empty response", body: `{"datasets": [{"dataset": "complete"}]}`},
		{name: "empty response", body: `{"datasets": []}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := NewWithT(t)
			ctx, cancel := context.WithCancel(context.Background())
			handler, writer := newPollTestHandler(t, "http://dashboard")
			handler.HttpClient = &http.Client{Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Header:     make(http.Header),
					Body: &cancelOnEOFBody{
						Reader: strings.NewReader(tc.body),
						cancel: cancel,
					},
					Request: req,
				}, nil
			})}

			outcome := handler.pollSingleEndpoint(ctx, dataDatasetsEndpointPrefix+"01000000", "session_1", false)

			g.Expect(outcome).To(Equal(pollFailed))
			g.Expect(ctx.Err()).To(MatchError(context.Canceled))
			g.Expect(writtenKeys(writer)).To(BeEmpty())
		})
	}
}

// TestPolledEndpointsAppendsConfiguredOnes verifies RAY_COLLECTOR_ADDITIONAL_ENDPOINTS adds
// to the fixed default set, deduplicated. Dynamic Ray Data endpoints are tested separately.
func TestPolledEndpointsAppendsConfiguredOnes(t *testing.T) {
	g := NewWithT(t)

	handler, _ := newPollTestHandler(t, "http://unused")
	g.Expect(handler.polledEndpoints()).To(Equal(defaultPolledEndpoints))

	handler.AdditionalEndpoints = []string{
		trainRunsEndpoint,
		serveApplicationsEndpoint, // already built in
		trainRunsEndpoint,         // repeated by the user
	}
	g.Expect(handler.polledEndpoints()).To(Equal([]string{
		serveApplicationsEndpoint,
		placementGroupsEndpoint,
		trainRunsEndpoint,
	}))

	// The built-in list itself must not be mutated by the append.
	g.Expect(defaultPolledEndpoints).To(Equal([]string{
		serveApplicationsEndpoint,
		placementGroupsEndpoint,
	}))
}

// TestPollAllEndpointsStoresConfiguredEndpoint verifies a configured endpoint is stored too.
func TestPollAllEndpointsStoresConfiguredEndpoint(t *testing.T) {
	g := NewWithT(t)

	dash := &fakeDashboard{jobs: `[]`}
	srv := dash.start(t)
	handler, writer := newPollTestHandler(t, srv.URL)
	handler.AdditionalEndpoints = []string{trainRunsEndpoint}

	handler.pollAllEndpoints(context.Background(), "session_1", newDatasetPollState(), false)

	g.Expect(dash.requestsFor(trainRunsEndpoint)).To(HaveLen(1))
	g.Expect(writtenKeys(writer)).To(ContainElement(
		"cluster-dir/session_1/fetched_endpoints/restful__api__train__v2__runs__v1"))
	g.Expect(writtenKeys(writer)).To(HaveLen(3))
}

// TestServeSnapshotFollowsLiveButSurvivesShutdown verifies a periodic poll mirrors the live
// cluster (empty included) while the final shutdown poll cannot erase a converged snapshot.
func TestServeSnapshotFollowsLiveButSurvivesShutdown(t *testing.T) {
	g := NewWithT(t)

	var mu sync.Mutex
	serveBody := `{"applications": {"app": {"status": "RUNNING"}}}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == jobsEndpoint {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		mu.Lock()
		defer mu.Unlock()
		_, _ = w.Write([]byte(serveBody))
	}))
	t.Cleanup(srv.Close)

	handler, writer := newPollTestHandler(t, srv.URL)
	serveKey := "cluster-dir/session_1/fetched_endpoints/" +
		utils.EndpointPathToStorageKey(serveApplicationsEndpoint)
	storedServe := func() string {
		writer.mu.Lock()
		defer writer.mu.Unlock()
		return writer.writtenFiles[serveKey]
	}
	setServeBody := func(body string) {
		mu.Lock()
		defer mu.Unlock()
		serveBody = body
	}

	handler.pollAllEndpoints(context.Background(), "session_1", newDatasetPollState(), false)
	g.Expect(storedServe()).To(ContainSubstring("RUNNING"))

	// A final poll must not erase the converged snapshot with a dying dashboard's answer.
	setServeBody(`{"applications": {}}`)
	handler.pollAllEndpoints(context.Background(), "session_1", newDatasetPollState(), true)
	g.Expect(storedServe()).To(ContainSubstring("RUNNING"))

	// A periodic poll mirrors the live cluster, empty included.
	handler.pollAllEndpoints(context.Background(), "session_1", newDatasetPollState(), false)
	g.Expect(storedServe()).To(Equal(`{"applications": {}}`))
}

func TestHasServeApplications(t *testing.T) {
	g := NewWithT(t)

	g.Expect(hasServeApplications([]byte(`{"applications": {}}`))).To(BeFalse())
	g.Expect(hasServeApplications([]byte(`{"applications": {"a": {}}}`))).To(BeTrue())
	g.Expect(hasServeApplications([]byte(`{}`))).To(BeFalse())
	// An unexpected shape is stored rather than silently dropped.
	g.Expect(hasServeApplications([]byte(`not json`))).To(BeTrue())
}

// TestIsEmptyPayloadOnlyGuardsKnownEndpoints verifies the guard never suppresses endpoints it
// does not understand.
func TestIsEmptyPayloadOnlyGuardsKnownEndpoints(t *testing.T) {
	g := NewWithT(t)

	// Serve is guarded only on the final shutdown poll; datasets always.
	g.Expect(isEmptyPayload(serveApplicationsEndpoint, []byte(`{"applications": {}}`), true)).To(BeTrue())
	g.Expect(isEmptyPayload(serveApplicationsEndpoint, []byte(`{"applications": {}}`), false)).To(BeFalse())
	g.Expect(isEmptyPayload(dataDatasetsEndpointPrefix+"01000000", []byte(`{"datasets": []}`), false)).To(BeTrue())
	g.Expect(isEmptyPayload(placementGroupsEndpoint, []byte(`{}`), true)).To(BeFalse())
	g.Expect(isEmptyPayload(trainRunsEndpoint, []byte(`{}`), true)).To(BeFalse())
}

func TestHasDatasets(t *testing.T) {
	g := NewWithT(t)

	g.Expect(hasDatasets([]byte(`{"datasets": []}`))).To(BeFalse())
	g.Expect(hasDatasets([]byte(`{"datasets": [{"dataset": "a"}]}`))).To(BeTrue())
	g.Expect(hasDatasets([]byte(`{}`))).To(BeFalse())
	// An unexpected shape is stored rather than silently dropped.
	g.Expect(hasDatasets([]byte(`not json`))).To(BeTrue())
}

// TestTerminalJobStatusesMatchRay guards against drift from Ray's JobStatus enum.
func TestTerminalJobStatusesMatchRay(t *testing.T) {
	g := NewWithT(t)

	for _, status := range []string{"SUCCEEDED", "FAILED", "STOPPED"} {
		g.Expect(terminalJobStatuses[status]).To(BeTrue(), "%s should be terminal", status)
	}
	for _, status := range []string{"PENDING", "RUNNING", ""} {
		g.Expect(terminalJobStatuses[status]).To(BeFalse(), "%q should not be terminal", status)
	}
}

// TestFakeDashboardJobsShapeMatchesRay documents the /api/jobs/ fields the collector depends on.
func TestFakeDashboardJobsShapeMatchesRay(t *testing.T) {
	g := NewWithT(t)

	var jobs []struct {
		JobID  string `json:"job_id"`
		Status string `json:"status"`
	}
	body := `[{"job_id": "01000000", "submission_id": "raysubmit_x", "status": "SUCCEEDED", "type": "SUBMISSION"}]`
	g.Expect(json.Unmarshal([]byte(body), &jobs)).To(Succeed())
	g.Expect(jobs).To(HaveLen(1))
	g.Expect(jobs[0].JobID).To(Equal("01000000"))
	g.Expect(jobs[0].Status).To(Equal("SUCCEEDED"))
}
