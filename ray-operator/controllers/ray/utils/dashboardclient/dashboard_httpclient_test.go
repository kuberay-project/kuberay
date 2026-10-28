package dashboardclient

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/jarcoal/httpmock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
)

const (
	runtimeEnvStr = `working_dir: "./"
pip:
- requests==2.26.0
- pendulum==2.1.2
conda:
  dependencies:
  - pytorch
  - torchvision
  - pip
  - pip:
    - pendulum
eager_install: false`
)

var _ = Describe("RayFrameworkGenerator", func() {
	var (
		rayJob             *rayv1.RayJob
		expectJobId        string
		errorJobId         string
		rayDashboardClient *RayDashboardClient
	)

	BeforeEach(func() {
		expectJobId = "raysubmit_test001"
		rayJob = &rayv1.RayJob{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "rayjob-sample",
				Namespace: "default",
			},
			Spec: rayv1.RayJobSpec{
				Entrypoint: "python samply.py",
				Metadata: map[string]string{
					"owner": "test1",
				},
				RuntimeEnvYAML: runtimeEnvStr,
			},
		}

		rayDashboardClient = &RayDashboardClient{}
		rayDashboardClient.dashboardURL = "http://127.0.0.1:8090"
		rayDashboardClient.client = &http.Client{}
	})

	It("Test ConvertRayJobToReq", func() {
		rayJobRequest, err := ConvertRayJobToReq(rayJob)
		Expect(err).ToNot(HaveOccurred())
		Expect(rayJobRequest.RuntimeEnv).To(HaveLen(4))
		Expect(rayJobRequest.RuntimeEnv["working_dir"]).To(Equal("./"))
	})

	It("Test ConvertRayJobToReq with EntrypointResources", func() {
		rayJobRequest, err := ConvertRayJobToReq(&rayv1.RayJob{
			Spec: rayv1.RayJobSpec{
				EntrypointResources: `{"r1": 0.1, "r2": 0.2}`,
				EntrypointNumCpus:   1.1,
				EntrypointNumGpus:   2.2,
			},
		})
		Expect(err).ToNot(HaveOccurred())
		Expect(rayJobRequest.NumCpus).To(Equal(float32(1.1)))
		Expect(rayJobRequest.NumGpus).To(Equal(float32(2.2)))
		Expect(rayJobRequest.Resources).To(Equal(map[string]float32{"r1": 0.1, "r2": 0.2}))
	})

	It("Test ConvertRayJobToReq with invalid EntrypointResources", func() {
		_, err := ConvertRayJobToReq(&rayv1.RayJob{
			Spec: rayv1.RayJobSpec{
				EntrypointResources: `{"r1": "string"}`,
			},
		})
		Expect(err).Should(MatchError(ContainSubstring("json: cannot unmarshal")))
	})

	It("Test submitting/getting rayJob", func() {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder(http.MethodPost, rayDashboardClient.dashboardURL+JobPath,
			func(_ *http.Request) (*http.Response, error) {
				body := &utiltypes.RayJobResponse{
					JobId: expectJobId,
				}
				bodyBytes, _ := json.Marshal(body)
				return httpmock.NewBytesResponse(200, bodyBytes), nil
			})
		httpmock.RegisterResponder(http.MethodGet, rayDashboardClient.dashboardURL+JobPath+expectJobId,
			func(_ *http.Request) (*http.Response, error) {
				body := &utiltypes.RayJobInfo{
					JobStatus:  rayv1.JobStatusRunning,
					Entrypoint: rayJob.Spec.Entrypoint,
					Metadata:   rayJob.Spec.Metadata,
				}
				bodyBytes, _ := json.Marshal(body)
				return httpmock.NewBytesResponse(200, bodyBytes), nil
			})
		httpmock.RegisterResponder(http.MethodGet, rayDashboardClient.dashboardURL+JobPath+errorJobId,
			func(_ *http.Request) (*http.Response, error) {
				// return a string in the body
				return httpmock.NewStringResponse(200, "Ray misbehaved and sent string, not JSON"), nil
			})

		jobId, err := rayDashboardClient.SubmitJob(context.TODO(), rayJob)
		Expect(err).ToNot(HaveOccurred())
		Expect(jobId).To(Equal(expectJobId))

		rayJobInfo, err := rayDashboardClient.GetJobInfo(context.TODO(), jobId)
		Expect(err).ToNot(HaveOccurred())
		Expect(rayJobInfo.Entrypoint).To(Equal(rayJob.Spec.Entrypoint))
		Expect(rayJobInfo.JobStatus).To(Equal(rayv1.JobStatusRunning))

		_, err = rayDashboardClient.GetJobInfo(context.TODO(), errorJobId)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("GetJobInfo fail"))
		Expect(err.Error()).To(ContainSubstring("Ray misbehaved"))
	})

	It("Test stop job", func() {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder(http.MethodPost, rayDashboardClient.dashboardURL+JobPath+"stop-job-1/stop",
			func(_ *http.Request) (*http.Response, error) {
				body := &utiltypes.RayJobStopResponse{
					Stopped: true,
				}
				bodyBytes, _ := json.Marshal(body)
				return httpmock.NewBytesResponse(200, bodyBytes), nil
			})

		err := rayDashboardClient.StopJob(context.TODO(), "stop-job-1")
		Expect(err).ToNot(HaveOccurred())
	})

	It("Test stop succeeded job", func() {
		// StopJob only returns an error when JobStatus is not in terminated states (STOPPED / SUCCEEDED / FAILED)
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()
		httpmock.RegisterResponder(http.MethodPost, rayDashboardClient.dashboardURL+JobPath+"stop-job-1/stop",
			func(_ *http.Request) (*http.Response, error) {
				body := &utiltypes.RayJobStopResponse{
					Stopped: false,
				}
				bodyBytes, _ := json.Marshal(body)
				return httpmock.NewBytesResponse(200, bodyBytes), nil
			})
		httpmock.RegisterResponder(http.MethodGet, rayDashboardClient.dashboardURL+JobPath+"stop-job-1",
			func(_ *http.Request) (*http.Response, error) {
				body := &utiltypes.RayJobInfo{
					JobStatus:  rayv1.JobStatusSucceeded,
					Entrypoint: rayJob.Spec.Entrypoint,
					Metadata:   rayJob.Spec.Metadata,
				}
				bodyBytes, _ := json.Marshal(body)
				return httpmock.NewBytesResponse(200, bodyBytes), nil
			})

		err := rayDashboardClient.StopJob(context.TODO(), "stop-job-1")
		Expect(err).ToNot(HaveOccurred())
	})

	It("Test GetServeDetails URL construction with query parameters", func() {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		// Mock the response
		expectedResponse := &utiltypes.ServeDetails{
			Applications: map[string]utiltypes.ServeApplicationDetails{
				"app1": {
					ServeApplicationStatus: utiltypes.ServeApplicationStatus{
						Name:   "app1",
						Status: "RUNNING",
					},
					Deployments: map[string]utiltypes.ServeDeploymentDetails{},
				},
			},
			DeployMode: "MULTI_APP",
		}
		responseBytes, _ := json.Marshal(expectedResponse)

		// Register responder that checks the URL includes the query parameter
		httpmock.RegisterResponder(http.MethodGet, rayDashboardClient.dashboardURL+ServeDetailsPath,
			func(req *http.Request) (*http.Response, error) {
				// Verify the query parameter is present
				Expect(req.URL.Query().Get("api_type")).To(Equal("declarative"))
				return httpmock.NewBytesResponse(200, responseBytes), nil
			})

		details, err := rayDashboardClient.GetServeDetails(context.TODO())
		Expect(err).ToNot(HaveOccurred())
		Expect(details.Applications).To(HaveKey("app1"))
		Expect(details.DeployMode).To(Equal("MULTI_APP"))
	})

	It("Test GetServeDetails with invalid dashboard URL", func() {
		invalidClient := &RayDashboardClient{
			dashboardURL: "://invalid-url",
			client:       &http.Client{},
		}

		_, err := invalidClient.GetServeDetails(context.TODO())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("failed to parse dashboard URL"))
	})

	It("Test GetServeDetails with HTTP 400 Bad Request", func() {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		// Test 400 Bad Request (might happen with older Ray versions)
		httpmock.RegisterResponder(http.MethodGet, rayDashboardClient.dashboardURL+ServeDetailsPath,
			func(_ *http.Request) (*http.Response, error) {
				return httpmock.NewStringResponse(400, "Bad Request: unknown parameter api_type"), nil
			})

		_, err := rayDashboardClient.GetServeDetails(context.TODO())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("GetServeDetails fail: 400"))
		Expect(err.Error()).To(ContainSubstring("Bad Request: unknown parameter api_type"))
	})

	It("Test GetServeDetails with HTTP 500 Internal Server Error", func() {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(http.MethodGet, rayDashboardClient.dashboardURL+ServeDetailsPath,
			func(_ *http.Request) (*http.Response, error) {
				return httpmock.NewStringResponse(500, "Internal Server Error"), nil
			})

		_, err := rayDashboardClient.GetServeDetails(context.TODO())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("GetServeDetails fail: 500"))
	})

	It("Test GetServeDetails with invalid JSON response", func() {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(http.MethodGet, rayDashboardClient.dashboardURL+ServeDetailsPath,
			func(_ *http.Request) (*http.Response, error) {
				return httpmock.NewStringResponse(200, "invalid json response"), nil
			})

		_, err := rayDashboardClient.GetServeDetails(context.TODO())
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("GetServeDetails failed. Failed to unmarshal bytes"))
		Expect(err.Error()).To(ContainSubstring("invalid json response"))
	})

	It("Test GetServeDetails with empty response", func() {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		// Test with empty but valid JSON
		emptyResponse := &utiltypes.ServeDetails{
			Applications: map[string]utiltypes.ServeApplicationDetails{},
			DeployMode:   "",
		}
		responseBytes, _ := json.Marshal(emptyResponse)

		httpmock.RegisterResponder(http.MethodGet, rayDashboardClient.dashboardURL+ServeDetailsPath,
			func(req *http.Request) (*http.Response, error) {
				// Verify the query parameter is present
				Expect(req.URL.Query().Get("api_type")).To(Equal("declarative"))
				return httpmock.NewBytesResponse(200, responseBytes), nil
			})

		details, err := rayDashboardClient.GetServeDetails(context.TODO())
		Expect(err).ToNot(HaveOccurred())
		Expect(details.Applications).To(BeEmpty())
		Expect(details.DeployMode).To(Equal(""))
	})

	It("Test GetServeDetails with network error", func() {
		httpmock.Activate()
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder(http.MethodGet, rayDashboardClient.dashboardURL+ServeDetailsPath,
			httpmock.NewErrorResponder(context.DeadlineExceeded))

		_, err := rayDashboardClient.GetServeDetails(context.TODO())
		Expect(err).To(HaveOccurred())
		Expect(errors.Is(err, context.DeadlineExceeded)).To(BeTrue())
	})
})
