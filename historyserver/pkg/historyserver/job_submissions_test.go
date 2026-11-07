package historyserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/stretchr/testify/require"

	"github.com/ray-project/kuberay/historyserver/pkg/eventserver"
	eventtypes "github.com/ray-project/kuberay/historyserver/pkg/eventserver/types"
	"github.com/ray-project/kuberay/historyserver/pkg/storage/clusterlogs"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const syntheticFailedSubmission = `[{"type":"SUBMISSION","submission_id":"sub-only","job_id":null,"driver_info":null,"status":"FAILED","entrypoint":"python synthetic.py","message":"synthetic failure","runtime_env":{"synthetic":"discard"}}]`

type submissionStorageReader struct {
	mockStorageReader
	objects      map[string]string
	contentReads int
}

func (r *submissionStorageReader) GetContent(prefix, filename string) io.Reader {
	r.contentReads++
	if body, ok := r.objects[path.Join(prefix, filename)]; ok {
		return strings.NewReader(body)
	}
	return nil
}

func TestSubmissionSnapshotOnlyAPI(t *testing.T) {
	info := testClusterInfo()
	prefix := clusterlogs.Prefix("", info)
	reader := &submissionStorageReader{objects: map[string]string{
		path.Join(prefix, info.SessionName, "fetched_endpoints/restful__api__jobs"): syntheticFailedSubmission,
	}}
	loader := newTestLoader(t, NewSessionProcessor(reader, newFakeK8sClient(t, nil), true), loaderTestConfig{})
	_, err := loader.LoadSession(context.Background(), info)
	require.NoError(t, err)
	restful.DefaultContainer = restful.NewContainer()
	handler := &ServerHandler{sessionLoader: loader}
	routerAPI(handler)
	reads := reader.contentReads
	for _, url := range []string{"/api/jobs/", "/api/jobs/sub-only"} {
		t.Run(url, func(t *testing.T) {
			resp := submissionAPIRequest(t, info, url)
			require.Equal(t, http.StatusOK, resp.Code)
			var row map[string]any
			if strings.HasSuffix(url, "/") {
				var rows []map[string]any
				require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &rows))
				require.Len(t, rows, 1, "snapshot-only submission must be listed")
				row = rows[0]
			} else {
				require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &row))
			}
			require.Equal(t, "FAILED", row["status"])
			require.Equal(t, "sub-only", row["submission_id"])
			for _, key := range []string{"job_id", "driver_info", "driver_exit_code", "start_time", "end_time", "runtime_env"} {
				require.Contains(t, row, key)
				require.Nil(t, row[key], key)
			}
		})
	}
	require.Equal(t, reads, reader.contentReads, "Jobs requests must not read storage")
}

func submissionAPIRequest(t *testing.T, info utils.ClusterInfo, url string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, url, nil)
	for key, value := range map[string]string{
		COOKIE_CLUSTER_NAME_KEY: info.Name, COOKIE_CLUSTER_NAMESPACE_KEY: info.Namespace, COOKIE_SESSION_NAME_KEY: info.SessionName,
	} {
		req.AddCookie(&http.Cookie{Name: key, Value: value})
	}
	resp := httptest.NewRecorder()
	restful.DefaultContainer.ServeHTTP(resp, req)
	return resp
}

func syntheticSubmission(id string, status eventtypes.JobStatus) eventtypes.JobDetails {
	return eventtypes.JobDetails{Type: "SUBMISSION", SubmissionID: replayPointer(id), Status: status, EntryPoint: "python synthetic.py"}
}

func replayDetails(t *testing.T, view jobReplayView, id string) eventtypes.JobDetails {
	t.Helper()
	value, ok := view.Lookup(id)
	require.True(t, ok)
	row, ok := value.(eventtypes.JobDetails)
	require.True(t, ok)
	return row
}

func TestSubmissionAPISharedViewAndLegacyFallback(t *testing.T) {
	info := testClusterInfo()
	core := "01000000"
	job := eventtypes.Job{JobID: core, SubmissionID: "sub", Status: eventtypes.JOBRUNNING, RuntimeEnv: map[string]string{"synthetic": "legacy"}}
	for _, withSnapshot := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy", true: "merged"}[withSnapshot], func(t *testing.T) {
			snap := &eventserver.SessionSnapshot{Jobs: map[string]eventtypes.Job{core: job}}
			if withSnapshot {
				base := syntheticSubmission("sub", eventtypes.JOBFAILED)
				base.JobID = replayPointer(core)
				snap.Submissions = map[string]eventtypes.JobDetails{"sub": base}
			}
			loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{})
			require.NoError(t, loader.putSnapshot(testClusterSessionKey(), snap, info, false))
			restful.DefaultContainer = restful.NewContainer()
			routerAPI(&ServerHandler{sessionLoader: loader})
			list := submissionAPIRequest(t, info, "/api/jobs/")
			require.Equal(t, http.StatusOK, list.Code)
			var rows []map[string]any
			require.NoError(t, json.Unmarshal(list.Body.Bytes(), &rows))
			require.Len(t, rows, 1)
			coreResp := submissionAPIRequest(t, info, "/api/jobs/"+core)
			require.Equal(t, http.StatusOK, coreResp.Code)
			encoded, err := json.Marshal(rows[0])
			require.NoError(t, err)
			require.JSONEq(t, string(encoded), coreResp.Body.String())
			submitResp := submissionAPIRequest(t, info, "/api/jobs/sub")
			if withSnapshot {
				require.JSONEq(t, coreResp.Body.String(), submitResp.Body.String())
				require.Equal(t, "FAILED", rows[0]["status"])
				require.Nil(t, rows[0]["runtime_env"])
			} else {
				legacy, err := json.Marshal(formatJobForResponse(job))
				require.NoError(t, err)
				require.JSONEq(t, string(legacy), coreResp.Body.String())
				require.Equal(t, "Job sub does not exist", submitResp.Body.String())
			}
			unknown := submissionAPIRequest(t, info, "/api/jobs/unknown")
			require.Equal(t, http.StatusOK, unknown.Code)
			require.Equal(t, "Job unknown does not exist", unknown.Body.String())
			loader.cache.Remove(testClusterSessionKey())
			for _, url := range []string{"/api/jobs/", "/api/jobs/sub"} {
				require.Equal(t, http.StatusServiceUnavailable, submissionAPIRequest(t, info, url).Code)
			}
		})
	}
}

func TestJobDetailLookupMatchesListView(t *testing.T) {
	core := "01000000"
	for _, state := range []string{"REST-only", "missing metadata", "matching metadata", "conflicting metadata"} {
		t.Run(state, func(t *testing.T) {
			first := syntheticSubmission("a", eventtypes.JOBFAILED)
			first.JobID = replayPointer(core)
			second := syntheticSubmission("b", eventtypes.PENDING)
			second.JobID = replayPointer(core)
			snap := &eventserver.SessionSnapshot{Submissions: map[string]eventtypes.JobDetails{
				"a": first, "b": second, core: syntheticSubmission(core, eventtypes.STOPPED),
			}}
			if state != "REST-only" {
				job := eventtypes.Job{JobID: core, Status: eventtypes.JOBRUNNING}
				if state == "matching metadata" {
					job.SubmissionID = "a"
				} else if state == "conflicting metadata" {
					job.SubmissionID = "other"
				}
				snap.Jobs = map[string]eventtypes.Job{core: job}
			}
			view := buildJobReplayView(snap)
			for _, id := range []string{core, "a", "b", "unknown"} {
				want, wantFound := view.Lookup(id)
				got, found := lookupJobReplay(snap, id)
				require.Equal(t, wantFound, found, id)
				require.Equal(t, want, got, id)
			}
		})
	}
}

func TestJobDetailAllocationsDoNotGrowWithEventJobs(t *testing.T) {
	previous := restful.DefaultContainer
	t.Cleanup(func() { restful.DefaultContainer = previous })
	for _, mode := range []string{"legacy", "core", "submission"} {
		t.Run(mode, func(t *testing.T) {
			info := testClusterInfo()
			var allocations []float64
			for _, size := range []int{1, 1000} {
				snap := &eventserver.SessionSnapshot{Jobs: make(map[string]eventtypes.Job, size)}
				for i := 0; i < size; i++ {
					id := fmt.Sprintf("%08x", i)
					snap.Jobs[id] = eventtypes.Job{JobID: id, Status: eventtypes.JOBRUNNING}
				}
				id := "00000000"
				if mode != "legacy" {
					base := syntheticSubmission("sub", eventtypes.JOBFAILED)
					if mode == "core" {
						base.JobID = replayPointer(id)
						job := snap.Jobs[id]
						job.SubmissionID = "sub"
						snap.Jobs[id] = job
					} else {
						id = "sub"
					}
					snap.Submissions = map[string]eventtypes.JobDetails{"sub": base}
				}
				loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{})
				require.NoError(t, loader.putSnapshot(testClusterSessionKey(), snap, info, false))
				restful.DefaultContainer = restful.NewContainer()
				routerAPI(&ServerHandler{sessionLoader: loader})
				allocations = append(allocations, testing.AllocsPerRun(3, func() {
					resp := submissionAPIRequest(t, info, "/api/jobs/"+id)
					require.Equal(t, http.StatusOK, resp.Code)
				}))
				runtime.KeepAlive(snap)
			}
			require.LessOrEqual(t, allocations[1], allocations[0]*2, "a detail request must not format unrelated event jobs")
		})
	}
}

func TestSubmissionStatusConflictMatrix(t *testing.T) {
	statuses := []eventtypes.JobStatus{eventtypes.PENDING, eventtypes.JOBRUNNING, eventtypes.SUCCEEDED, eventtypes.JOBFAILED, eventtypes.STOPPED}
	for _, restStatus := range statuses {
		for _, eventStatus := range append(statuses, "") {
			t.Run(string(restStatus)+"/"+string(eventStatus), func(t *testing.T) {
				base := syntheticSubmission("sub", restStatus)
				base.Message, base.ErrorType = replayPointer("rest message"), replayPointer("rest error")
				base.EndTime, base.DriverExitCode = replayPointer(int64(100)), replayPointer(8)
				job := eventtypes.Job{JobID: "01000000", SubmissionID: "sub", Status: eventStatus,
					Message: "event message", ErrorType: "event error", EndTime: time.UnixMilli(200), DriverExitCode: 9}
				got, conflict := mergeSubmission(base, &job)
				restTerminal := eventtypes.IsTerminalJobStatus(restStatus)
				eventTerminal := eventtypes.IsTerminalJobStatus(eventStatus)
				want := restStatus
				if !restTerminal && (eventTerminal || eventStatus == eventtypes.JOBRUNNING) {
					want = eventStatus
				}
				require.Equal(t, want, got.Status)
				require.Equal(t, restTerminal && eventTerminal && restStatus != eventStatus, conflict)
				if !restTerminal && eventTerminal {
					require.Equal(t, "event message", *got.Message)
					require.Equal(t, "event error", *got.ErrorType)
					require.Equal(t, int64(200), *got.EndTime)
					require.Equal(t, 9, *got.DriverExitCode)
				} else {
					require.Equal(t, base.Message, got.Message)
					require.Equal(t, base.ErrorType, got.ErrorType)
					require.Equal(t, base.EndTime, got.EndTime)
					require.Equal(t, base.DriverExitCode, got.DriverExitCode)
				}
			})
		}
	}
}

func TestSubmissionDoesNotBorrowExitCodeWhileNonTerminal(t *testing.T) {
	base := syntheticSubmission("sub", eventtypes.JOBRUNNING)
	got, conflict := mergeSubmission(base, &eventtypes.Job{
		JobID: "01000000", SubmissionID: "sub", Status: eventtypes.JOBRUNNING,
		DriverExitCode: 9, EndTime: time.UnixMilli(200),
	})
	require.False(t, conflict)
	require.Equal(t, eventtypes.JOBRUNNING, got.Status)
	require.Nil(t, got.DriverExitCode, "a driver that is still running cannot supply an exit code")
	require.Nil(t, got.EndTime, "end time follows the same terminal-only rule")
}

func TestSubmissionNullableEvidence(t *testing.T) {
	for _, tc := range []struct {
		name string
		job  eventtypes.Job
		want eventtypes.JobStatus
		exit *int
		end  *int64
	}{
		{"finished is not success", eventtypes.Job{State: eventtypes.JOBFINISHED, EndTime: time.UnixMilli(200)}, eventtypes.PENDING, nil, nil},
		{"created is not explicit running", eventtypes.Job{State: eventtypes.CREATED}, eventtypes.PENDING, nil, nil},
		{"running unknown zero", eventtypes.Job{Status: eventtypes.JOBRUNNING}, eventtypes.JOBRUNNING, nil, nil},
		{"failed unknown zero", eventtypes.Job{Status: eventtypes.JOBFAILED}, eventtypes.JOBFAILED, nil, nil},
		{"stopped unknown zero", eventtypes.Job{Status: eventtypes.STOPPED}, eventtypes.STOPPED, nil, nil},
		{"failed known nonzero", eventtypes.Job{Status: eventtypes.JOBFAILED, DriverExitCode: 2, EndTime: time.UnixMilli(200)}, eventtypes.JOBFAILED, replayPointer(2), replayPointer(int64(200))},
		{"succeeded unknown zero", eventtypes.Job{Status: eventtypes.SUCCEEDED}, eventtypes.SUCCEEDED, nil, nil},
		{"latest transition wins", eventtypes.Job{Status: eventtypes.JOBFAILED, StatusTransitions: []eventtypes.JobStatusTransition{{Status: eventtypes.SUCCEEDED}, {Status: eventtypes.JOBRUNNING}}}, eventtypes.JOBRUNNING, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := syntheticSubmission("sub", eventtypes.PENDING)
			got, _ := mergeSubmission(base, &tc.job)
			require.Equal(t, tc.want, got.Status)
			require.Equal(t, tc.exit, got.DriverExitCode)
			require.Equal(t, tc.end, got.EndTime)
			require.Nil(t, got.StartTime)
		})
	}
	base := syntheticSubmission("sub", eventtypes.SUCCEEDED)
	base.DriverExitCode = replayPointer(0)
	got, conflict := mergeSubmission(base, &eventtypes.Job{Status: eventtypes.SUCCEEDED})
	require.False(t, conflict)
	require.NotNil(t, got.DriverExitCode)
	require.Equal(t, 0, *got.DriverExitCode)

	base = syntheticSubmission("sub", eventtypes.JOBFAILED)
	job := eventtypes.Job{Status: eventtypes.SUCCEEDED, EndTime: time.UnixMilli(200)}
	got, conflict = mergeSubmission(base, &job)
	require.True(t, conflict)
	require.Nil(t, got.DriverExitCode)
	require.Nil(t, got.EndTime)
}

func TestExplicitJobStatusFallsBackWhenLastTransitionIsEmpty(t *testing.T) {
	job := eventtypes.Job{
		Status:            eventtypes.JOBFAILED,
		StatusTransitions: []eventtypes.JobStatusTransition{{Status: eventtypes.SUCCEEDED}, {Status: ""}},
	}
	require.Equal(t, eventtypes.JOBFAILED, explicitJobStatus(job))
}

func TestSubmissionMergesExplicitDriverIdentity(t *testing.T) {
	base := syntheticSubmission("sub", eventtypes.PENDING)
	base.JobID = replayPointer("01000000")
	job := eventtypes.Job{JobID: "01000000", SubmissionID: "sub", Status: eventtypes.SUCCEEDED,
		DriverNodeIPAddress: "192.0.2.1", DriverPID: "10",
		DriverNodeID: "driver-node", DriverAgentHttpAddress: "http://192.0.2.1:52365",
		StartTime: time.UnixMilli(100), EndTime: time.UnixMilli(300), DriverExitCode: 4}
	snap := &eventserver.SessionSnapshot{
		Submissions: map[string]eventtypes.JobDetails{"sub": base},
		Jobs:        map[string]eventtypes.Job{"01000000": job},
	}
	view := buildJobReplayView(snap)
	require.Len(t, view.Rows, 1)
	row := replayDetails(t, view, "sub")
	require.Equal(t, base.JobID, row.JobID)
	require.Equal(t, eventtypes.SUCCEEDED, row.Status)
	require.Equal(t, eventtypes.JobDriverInfo{ID: "01000000", NodeIPAddress: "192.0.2.1", PID: "10"}, *row.DriverInfo)
	require.Equal(t, "driver-node", *row.DriverNodeID)
	require.Equal(t, "http://192.0.2.1:52365", *row.DriverAgentHTTPAddress)
	require.Equal(t, replayPointer(int64(100)), row.StartTime)
	require.Equal(t, replayPointer(int64(300)), row.EndTime)
	require.Equal(t, replayPointer(4), row.DriverExitCode)
	require.Equal(t, row, replayDetails(t, view, "01000000"))
	require.Equal(t, base, snap.Submissions["sub"], "merging must not change the cached submission")
	require.Equal(t, job, snap.Jobs["01000000"])

	unmatched := eventtypes.Job{JobID: "02000000", SubmissionID: "sub", Status: eventtypes.JOBFAILED}
	snap.Jobs[unmatched.JobID] = unmatched
	view = buildJobReplayView(snap)
	require.Len(t, view.Rows, 2)
	require.Equal(t, row, replayDetails(t, view, "sub"), "another event driver does not change the REST association")
	legacy, ok := view.Lookup(unmatched.JobID)
	require.True(t, ok)
	require.Equal(t, formatJobForResponse(unmatched), legacy)
	require.Contains(t, view.Rows, legacy)
}

func TestSubmissionCoreLookupPrecedesSubmissionID(t *testing.T) {
	job := eventtypes.Job{JobID: "01000000", Status: eventtypes.JOBRUNNING}
	snap := &eventserver.SessionSnapshot{
		Submissions: map[string]eventtypes.JobDetails{"01000000": syntheticSubmission("01000000", eventtypes.JOBFAILED)},
		Jobs:        map[string]eventtypes.Job{job.JobID: job},
	}
	view := buildJobReplayView(snap)
	require.Len(t, view.Rows, 2)
	got, ok := view.Lookup(job.JobID)
	require.True(t, ok)
	require.Equal(t, formatJobForResponse(job), got)
	require.Equal(t, eventtypes.JOBFAILED, view.bySubmit[job.JobID].(eventtypes.JobDetails).Status)
}

func TestSubmissionPreservesUnmatchedEventJobs(t *testing.T) {
	info := testClusterInfo()
	snap := &eventserver.SessionSnapshot{
		Submissions: map[string]eventtypes.JobDetails{"A": syntheticSubmission("A", eventtypes.JOBFAILED)},
		Jobs: map[string]eventtypes.Job{
			"01000000": {JobID: "01000000", SubmissionID: "B", Status: eventtypes.JOBFAILED,
				StartTime: time.UnixMilli(100), EndTime: time.UnixMilli(200), DriverExitCode: 7,
				RuntimeEnv: map[string]string{"synthetic": "event"}},
			"02000000": {JobID: "02000000", Status: eventtypes.JOBRUNNING,
				Config: eventtypes.DriverJobConfig{Metadata: map[string]string{"job_submission_id": "B"}}},
			"03000000": {JobID: "03000000", JobType: "DRIVER", Status: eventtypes.JOBRUNNING},
		},
	}
	before, err := json.Marshal(snap)
	require.NoError(t, err)
	loader := newTestLoader(t, &fakeProcessor{}, loaderTestConfig{})
	require.NoError(t, loader.putSnapshot(testClusterSessionKey(), snap, info, false))
	restful.DefaultContainer = restful.NewContainer()
	routerAPI(&ServerHandler{sessionLoader: loader})
	list := submissionAPIRequest(t, info, "/api/jobs/")
	require.Equal(t, http.StatusOK, list.Code)
	var rows []map[string]any
	require.NoError(t, json.Unmarshal(list.Body.Bytes(), &rows))
	require.Len(t, rows, 4)
	view := buildJobReplayView(snap)
	for core, job := range snap.Jobs {
		legacy := formatJobForResponse(job)
		require.Contains(t, view.Rows, legacy)
		encoded, err := json.Marshal(legacy)
		require.NoError(t, err)
		resp := submissionAPIRequest(t, info, "/api/jobs/"+core)
		require.Equal(t, http.StatusOK, resp.Code)
		require.JSONEq(t, string(encoded), resp.Body.String())
	}
	require.Equal(t, "Job B does not exist", submissionAPIRequest(t, info, "/api/jobs/B").Body.String())
	cached, ok := loader.GetSnapshot(testClusterSessionKey())
	require.True(t, ok)
	after, err := json.Marshal(cached)
	require.NoError(t, err)
	require.Equal(t, before, after, "replay must not mutate the cached snapshot")
}

func TestSubmissionAssociatesOnlyExplicitRESTCore(t *testing.T) {
	for _, tc := range []struct {
		name, restCore, explicit, metadata string
		wantRows                           int
		wantStatus                         eventtypes.JobStatus
	}{
		{"REST core links missing metadata", "01000000", "", "", 1, eventtypes.JOBFAILED},
		{"explicit wins over metadata", "01000000", "sub", "other", 1, eventtypes.JOBFAILED},
		{"matching metadata", "01000000", "", "sub", 1, eventtypes.JOBFAILED},
		{"no core is inferred from explicit submission", "", "sub", "", 2, eventtypes.JOBRUNNING},
		{"no core is inferred from metadata", "", "", "sub", 2, eventtypes.JOBRUNNING},
		{"explicit prevents REST reassociation", "01000000", "other", "sub", 2, eventtypes.JOBRUNNING},
		{"metadata prevents REST reassociation", "01000000", "", "other", 2, eventtypes.JOBRUNNING},
		{"different REST core", "02000000", "sub", "", 2, eventtypes.JOBRUNNING},
		{"core ordering is irrelevant", "00010000", "sub", "", 2, eventtypes.JOBRUNNING},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := syntheticSubmission("sub", eventtypes.JOBRUNNING)
			if tc.restCore != "" {
				base.JobID = replayPointer(tc.restCore)
			}
			job := eventtypes.Job{JobID: "01000000", SubmissionID: tc.explicit,
				Config: eventtypes.DriverJobConfig{Metadata: map[string]string{"job_submission_id": tc.metadata}}, Status: eventtypes.JOBFAILED}
			snap := &eventserver.SessionSnapshot{Submissions: map[string]eventtypes.JobDetails{"sub": base},
				Jobs: map[string]eventtypes.Job{job.JobID: job}}
			view := buildJobReplayView(snap)
			require.Len(t, view.Rows, tc.wantRows)
			got := replayDetails(t, view, "sub")
			require.Equal(t, base.JobID, got.JobID, "REST determines the driver identity")
			require.Equal(t, tc.wantStatus, got.Status)
			for _, id := range []string{"sub", job.JobID, tc.restCore} {
				want, wantFound := view.Lookup(id)
				row, found := lookupJobReplay(snap, id)
				require.Equal(t, wantFound, found, id)
				require.Equal(t, want, row, id)
			}
			if tc.wantRows == 1 {
				require.Equal(t, got, replayDetails(t, view, job.JobID))
			} else {
				coreRow, ok := view.Lookup(job.JobID)
				require.True(t, ok)
				require.Equal(t, formatJobForResponse(job), coreRow)
			}
			if tc.restCore != "" && tc.restCore != job.JobID {
				require.Equal(t, got, replayDetails(t, view, tc.restCore))
			}
		})
	}
}
