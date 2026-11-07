package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	rpcStatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/protobuf/encoding/protojson"

	apiserversdkutil "github.com/ray-project/kuberay/apiserversdk/util"
	api "github.com/ray-project/kuberay/proto/go_client"
)

type KuberayAPIServerClient struct {
	httpClient  *http.Client
	marshaler   *protojson.MarshalOptions
	unmarshaler *protojson.UnmarshalOptions
	// TODO(hjiang): here we use function to allow customized http request handling logic in unit test, worth revisiting if there're better ways;
	// for example, (1) wrap an interface to process request; (2) inject round-trip logic into http client.
	// See https://github.com/ray-project/kuberay/pull/3334/files#r2041183495 for details.
	//
	// Store http request handling function for unit test purpose.
	ExecuteHttpRequest func(httpRequest *http.Request, URL string) ([]byte, *rpcStatus.Status, error)
	baseURL            string
	retryCfg           apiserversdkutil.RetryConfig
}

type KuberayAPIServerClientError struct {
	HTTPStatusCode int
}

func (krce *KuberayAPIServerClientError) Error() string {
	return fmt.Sprintf("kuberay api server request failed with HTTP status (%d: %s)", krce.HTTPStatusCode, http.StatusText(krce.HTTPStatusCode))
}

func IsNotFoundError(err error) bool {
	if err != nil {
		apiServerError := &KuberayAPIServerClientError{}
		if errors.As(err, &apiServerError); apiServerError.HTTPStatusCode == http.StatusNotFound {
			return true
		}
	}
	return false
}

func NewKuberayAPIServerClient(baseURL string, httpClient *http.Client, retryCfg apiserversdkutil.RetryConfig) *KuberayAPIServerClient {
	client := &KuberayAPIServerClient{
		httpClient: httpClient,
		baseURL:    baseURL,
		marshaler: &protojson.MarshalOptions{
			Multiline:       true,
			Indent:          "    ",
			AllowPartial:    false,
			UseProtoNames:   true,
			UseEnumNumbers:  false,
			EmitUnpopulated: false,
			Resolver:        nil,
		},
		unmarshaler: &protojson.UnmarshalOptions{
			AllowPartial:   false,
			DiscardUnknown: false,
			Resolver:       nil,
		},
		retryCfg: retryCfg,
	}
	client.ExecuteHttpRequest = client.executeRequest
	return client
}

// Setter function for setting executeHttpRequest method
func (krc *KuberayAPIServerClient) SetExecuteHttpRequest(fn func(httpRequest *http.Request, URL string) ([]byte, *rpcStatus.Status, error)) {
	krc.ExecuteHttpRequest = fn
}

// CreateComputeTemplate creates a new compute template.
func (krc *KuberayAPIServerClient) CreateComputeTemplate(request *api.CreateComputeTemplateRequest) (*api.ComputeTemplate, *rpcStatus.Status, error) {
	createURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/compute_templates"

	bytez, err := krc.marshaler.Marshal(request.ComputeTemplate)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal api.ComputeTemplate to JSON: %w", err)
	}

	httpRequest, err := http.NewRequestWithContext(context.TODO(), "POST", createURL, bytes.NewReader(bytez))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", createURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")
	httpRequest.Header.Add("Content-Type", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, createURL)
	if err != nil {
		return nil, status, err
	}
	computeTemplate := &api.ComputeTemplate{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, computeTemplate); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}

	return computeTemplate, nil, nil
}

// DeleteComputeTemplate deletes a compute template.
func (krc *KuberayAPIServerClient) DeleteComputeTemplate(request *api.DeleteComputeTemplateRequest) (*rpcStatus.Status, error) {
	deleteURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/compute_templates/" + request.Name
	return krc.doDelete(deleteURL)
}

// Finds a specific compute template by its name and namespace.
func (krc *KuberayAPIServerClient) GetComputeTemplate(request *api.GetComputeTemplateRequest) (*api.ComputeTemplate, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/compute_templates/" + request.Name
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	computeTemplate := &api.ComputeTemplate{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, computeTemplate); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return computeTemplate, nil, nil
}

// GetAllComputeTemplates finds all compute templates in all namespaces.
func (krc *KuberayAPIServerClient) GetAllComputeTemplates() (*api.ListAllComputeTemplatesResponse, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/compute_templates"
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.ListAllComputeTemplatesResponse{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// GetAllComputeTemplatesInNamespace Finds all compute templates in a given namespace.
func (krc *KuberayAPIServerClient) GetAllComputeTemplatesInNamespace(request *api.ListComputeTemplatesRequest) (*api.ListComputeTemplatesResponse, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/compute_templates"
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.ListComputeTemplatesResponse{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// CreateCluster creates a new cluster.
func (krc *KuberayAPIServerClient) CreateCluster(request *api.CreateClusterRequest) (*api.Cluster, *rpcStatus.Status, error) {
	createURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/clusters"

	bytez, err := krc.marshaler.Marshal(request.Cluster)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal api.Cluster to JSON: %w", err)
	}

	httpRequest, err := http.NewRequestWithContext(context.TODO(), "POST", createURL, bytes.NewReader(bytez))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", createURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")
	httpRequest.Header.Add("Content-Type", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, createURL)
	if err != nil {
		return nil, status, err
	}
	cluster := &api.Cluster{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, cluster); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return cluster, nil, nil
}

// DeleteCluster deletes a cluster
func (krc *KuberayAPIServerClient) DeleteCluster(request *api.DeleteClusterRequest) (*rpcStatus.Status, error) {
	deleteURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/clusters/" + request.Name
	return krc.doDelete(deleteURL)
}

// GetCluster finds a specific Cluster by ID.
func (krc *KuberayAPIServerClient) GetCluster(request *api.GetClusterRequest) (*api.Cluster, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/clusters/" + request.Name
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	cluster := &api.Cluster{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, cluster); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return cluster, nil, nil
}

// ListCluster finds all clusters in a given namespace.
func (krc *KuberayAPIServerClient) ListClusters(request *api.ListClustersRequest) (*api.ListClustersResponse, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/clusters"
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	q := httpRequest.URL.Query()
	q.Set("limit", strconv.FormatInt(request.Limit, 10))
	q.Set("continue", request.Continue)
	httpRequest.URL.RawQuery = q.Encode()

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.ListClustersResponse{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// ListAllClusters finds all Clusters in all namespaces.
func (krc *KuberayAPIServerClient) ListAllClusters(request *api.ListAllClustersRequest) (*api.ListAllClustersResponse, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/clusters"
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	q := httpRequest.URL.Query()
	q.Set("limit", strconv.FormatInt(request.Limit, 10))
	q.Set("continue", request.Continue)
	httpRequest.URL.RawQuery = q.Encode()

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.ListAllClustersResponse{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// CreateRayJob creates a new job.
func (krc *KuberayAPIServerClient) CreateRayJob(request *api.CreateRayJobRequest) (*api.RayJob, *rpcStatus.Status, error) {
	createURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/jobs"
	bytez, err := krc.marshaler.Marshal(request.Job)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal api.Cluster to JSON: %w", err)
	}

	httpRequest, err := http.NewRequestWithContext(context.TODO(), "POST", createURL, bytes.NewReader(bytez))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", createURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")
	httpRequest.Header.Add("Content-Type", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, createURL)
	if err != nil {
		return nil, status, err
	}
	rayJob := &api.RayJob{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, rayJob); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return rayJob, nil, nil
}

// GetRayJob finds a specific job by its name and namespace.
func (krc *KuberayAPIServerClient) GetRayJob(request *api.GetRayJobRequest) (*api.RayJob, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/jobs/" + request.Name
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	rayJob := &api.RayJob{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, rayJob); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return rayJob, nil, nil
}

// Finds all job in a given namespace.
func (krc *KuberayAPIServerClient) ListRayJobs(request *api.ListRayJobsRequest) (*api.ListRayJobsResponse, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/jobs"
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	q := httpRequest.URL.Query()
	q.Set("limit", strconv.FormatInt(request.Limit, 10))
	q.Set("continue", request.Continue)
	httpRequest.URL.RawQuery = q.Encode()

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.ListRayJobsResponse{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// ListAllRayJobs Finds all job in all namespaces.
func (krc *KuberayAPIServerClient) ListAllRayJobs(request *api.ListAllRayJobsRequest) (*api.ListAllRayJobsResponse, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/jobs"
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	q := httpRequest.URL.Query()
	q.Set("limit", strconv.FormatInt(request.Limit, 10))
	q.Set("continue", request.Continue)
	httpRequest.URL.RawQuery = q.Encode()
	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.ListAllRayJobsResponse{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// Deletes a job by its name and namespace.
func (krc *KuberayAPIServerClient) DeleteRayJob(request *api.DeleteRayJobRequest) (*rpcStatus.Status, error) {
	deleteURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/jobs/" + request.Name
	return krc.doDelete(deleteURL)
}

// CreateRayService create a new ray serve.
func (krc *KuberayAPIServerClient) CreateRayService(request *api.CreateRayServiceRequest) (*api.RayService, *rpcStatus.Status, error) {
	createURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/services"
	bytez, err := krc.marshaler.Marshal(request.Service)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal api.Cluster to JSON: %w", err)
	}

	httpRequest, err := http.NewRequestWithContext(context.TODO(), "POST", createURL, bytes.NewReader(bytez))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", createURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")
	httpRequest.Header.Add("Content-Type", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, createURL)
	if err != nil {
		return nil, status, err
	}
	rayService := &api.RayService{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, rayService); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return rayService, nil, nil
}

// UpdateRayService updates a ray serve service.
func (krc *KuberayAPIServerClient) UpdateRayService(request *api.UpdateRayServiceRequest) (*api.RayService, *rpcStatus.Status, error) {
	updateURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/services/" + request.Name
	bytez, err := krc.marshaler.Marshal(request.Service)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal api.Cluster to JSON: %w", err)
	}

	httpRequest, err := http.NewRequestWithContext(context.TODO(), "PUT", updateURL, bytes.NewReader(bytez))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", updateURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")
	httpRequest.Header.Add("Content-Type", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, updateURL)
	if err != nil {
		return nil, status, err
	}
	rayService := &api.RayService{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, rayService); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return rayService, nil, nil
}

// Find a specific ray serve by name and namespace.
func (krc *KuberayAPIServerClient) GetRayService(request *api.GetRayServiceRequest) (*api.RayService, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/services/" + request.Name
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.RayService{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// Finds all ray services in a given namespace.
func (krc *KuberayAPIServerClient) ListRayServices(request *api.ListRayServicesRequest) (*api.ListRayServicesResponse, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/services"
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	q := httpRequest.URL.Query()
	q.Set("pageSize", strconv.FormatInt(int64(request.PageSize), 10))
	q.Set("pageToken", request.PageToken)
	httpRequest.URL.RawQuery = q.Encode()
	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.ListRayServicesResponse{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}

	return response, nil, nil
}

// Finds all ray services in all namespaces.
func (krc *KuberayAPIServerClient) ListAllRayServices(request *api.ListAllRayServicesRequest) (*api.ListAllRayServicesResponse, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/services"
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	q := httpRequest.URL.Query()
	q.Set("pageSize", strconv.FormatInt(int64(request.PageSize), 10))
	q.Set("pageToken", request.PageToken)
	httpRequest.URL.RawQuery = q.Encode()
	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.ListAllRayServicesResponse{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// DeleteRayService deletes a ray service by its name and namespace
func (krc *KuberayAPIServerClient) DeleteRayService(request *api.DeleteRayServiceRequest) (*rpcStatus.Status, error) {
	deleteURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/services/" + request.Name
	return krc.doDelete(deleteURL)
}

// SubmitRayJob creates a new job on a given cluster.
func (krc *KuberayAPIServerClient) SubmitRayJob(request *api.SubmitRayJobRequest) (*api.SubmitRayJobReply, *rpcStatus.Status, error) {
	createURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/jobsubmissions/" + request.Clustername
	bytez, err := krc.marshaler.Marshal(request.Jobsubmission)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal api.Cluster to JSON: %w", err)
	}

	httpRequest, err := http.NewRequestWithContext(context.TODO(), "POST", createURL, bytes.NewReader(bytez))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", createURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")
	httpRequest.Header.Add("Content-Type", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, createURL)
	if err != nil {
		return nil, status, err
	}
	submission := &api.SubmitRayJobReply{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, submission); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return submission, nil, nil
}

// GetRayJobDetails. Get details about specific job on a given cluster.
func (krc *KuberayAPIServerClient) GetRayJobDetails(request *api.GetJobDetailsRequest) (*api.JobSubmissionInfo, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/jobsubmissions/" + request.Clustername + "/" + request.Submissionid
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.JobSubmissionInfo{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// GetRayJobLog. Get log for a specific job on a given cluster.
func (krc *KuberayAPIServerClient) GetRayJobLog(request *api.GetJobLogRequest) (*api.GetJobLogReply, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/jobsubmissions/" + request.Clustername + "/log/" + request.Submissionid
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.GetJobLogReply{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// ListRayJobsCluster. List Ray jobs on a given cluster.
func (krc *KuberayAPIServerClient) ListRayJobsCluster(request *api.ListJobDetailsRequest) (*api.ListJobSubmissionInfo, *rpcStatus.Status, error) {
	getURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/jobsubmissions/" + request.Clustername
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "GET", getURL, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create http request for url '%s': %w", getURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")

	bodyBytes, status, err := krc.ExecuteHttpRequest(httpRequest, getURL)
	if err != nil {
		return nil, status, err
	}
	response := &api.ListJobSubmissionInfo{}
	if err := krc.unmarshaler.Unmarshal(bodyBytes, response); err != nil {
		return nil, status, fmt.Errorf("failed to unmarshal: %+w", err)
	}
	return response, nil, nil
}

// StopRayJob stops job on a given cluster.
func (krc *KuberayAPIServerClient) StopRayJob(request *api.StopRayJobSubmissionRequest) (*rpcStatus.Status, error) {
	createURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/jobsubmissions/" + request.Clustername + "/" + request.Submissionid

	httpRequest, err := http.NewRequestWithContext(context.TODO(), "POST", createURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request for url '%s': %w", createURL, err)
	}

	httpRequest.Header.Add("Accept", "application/json")
	httpRequest.Header.Add("Content-Type", "application/json")

	_, status, err := krc.ExecuteHttpRequest(httpRequest, createURL)
	if err != nil {
		return status, err
	}
	return nil, nil
}

// DeleteRayService deletes a ray service by its name and namespace
func (krc *KuberayAPIServerClient) DeleteRayJobCluster(request *api.DeleteRayJobSubmissionRequest) (*rpcStatus.Status, error) {
	deleteURL := krc.baseURL + "/apis/v1/namespaces/" + request.Namespace + "/jobsubmissions/" + request.Clustername + "/" + request.Submissionid
	return krc.doDelete(deleteURL)
}

func (krc *KuberayAPIServerClient) doDelete(deleteURL string) (*rpcStatus.Status, error) {
	httpRequest, err := http.NewRequestWithContext(context.TODO(), "DELETE", deleteURL, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create http request for url '%s': %w", deleteURL, err)
	}
	httpRequest.Header.Add("Accept", "application/json")
	_, status, err := krc.ExecuteHttpRequest(httpRequest, deleteURL)
	return status, err
}

func (krc *KuberayAPIServerClient) executeRequest(httpRequest *http.Request, URL string) ([]byte, *rpcStatus.Status, error) {
	// Set the overall timeout
	ctx, cancel := context.WithTimeout(context.Background(), krc.retryCfg.OverallTimeout)
	defer cancel()

	httpRequest = httpRequest.WithContext(ctx)

	// Record the last error and status got
	var lastErr error
	var lastStatus *rpcStatus.Status

	var requestBodyBytes []byte
	var err error
	if httpRequest.Body != nil {
		requestBodyBytes, err = io.ReadAll(httpRequest.Body)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read request body: %w", err)
		}
	}

	doReq := func() ([]byte, int, error) {
		// new ReadCloser for httpRequest body
		if requestBodyBytes != nil {
			httpRequest.Body = io.NopCloser(bytes.NewBuffer(requestBodyBytes))
		}

		response, err := krc.httpClient.Do(httpRequest)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to execute http request for url '%s': %w", URL, err)
		}
		defer response.Body.Close()

		bodyBytes, err := io.ReadAll(response.Body)
		// Error in reading response body, treated as non-retryable error
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read response body bytes: %w", err)
		}

		return bodyBytes, response.StatusCode, nil
	}

	// Only retry for HTTP status codes defined as retryable in isRetryableHTTPStatus().
	for attempt := 0; attempt <= krc.retryCfg.MaxRetry; attempt++ {
		bodyBytes, statusCode, err := doReq()
		// Error in sending the request, treated as non-retryable error
		if err != nil {
			lastStatus = nil
			lastErr = err
			break
		}

		if apiserversdkutil.IsSuccessfulStatusCode(statusCode) {
			return bodyBytes, nil, nil
		}

		status, err := krc.extractStatus(bodyBytes)
		// Error in extracting status from response body, treated as non-retryable error
		if err != nil {
			lastStatus = nil
			lastErr = err
			break
		}

		lastStatus = status
		lastErr = &KuberayAPIServerClientError{
			HTTPStatusCode: statusCode,
		}

		if !apiserversdkutil.IsRetryableHTTPStatusCodes(statusCode) {
			break
		}

		// Backoff before retry
		sleep := apiserversdkutil.GetRetryBackoff(attempt,
			krc.retryCfg.InitBackoff,
			krc.retryCfg.BackoffFactor,
			krc.retryCfg.MaxBackoff)

		if ok := apiserversdkutil.CheckContextDeadline(ctx, sleep); !ok {
			return nil, lastStatus, fmt.Errorf("retry timeout exceeded context deadline")
		}

		if err = apiserversdkutil.Sleep(ctx, sleep); err != nil {
			return nil, lastStatus, fmt.Errorf("retry canceled during backoff: %w", err)
		}

	}
	return nil, lastStatus, lastErr
}

func (krc *KuberayAPIServerClient) extractStatus(bodyBytes []byte) (*rpcStatus.Status, error) {
	status := &rpcStatus.Status{}
	err := krc.unmarshaler.Unmarshal(bodyBytes, status)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal status object: %w", err)
	}
	return status, nil
}
