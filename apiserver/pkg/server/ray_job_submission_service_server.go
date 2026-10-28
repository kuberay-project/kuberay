package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	"github.com/go-logr/zerologr"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/emptypb"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/yaml"

	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/dashboardclient"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
)

type RayJobSubmissionServiceServerOptions struct {
	CollectMetrics bool
}

// implements `type ClusterServiceServer interface` in cluster_grpc.pb.go
// ClusterServer is the server API for ClusterService service.
type RayJobSubmissionServiceServer struct {
	api.UnimplementedRayJobSubmissionServiceServer
	options             *RayJobSubmissionServiceServerOptions
	clusterServer       *ClusterServer
	dashboardClientFunc func(rayCluster *rayv1.RayCluster, url string) (dashboardclient.RayDashboardClientInterface, error)
	log                 logr.Logger
}

// Create RayJobSubmissionServiceServer
func NewRayJobSubmissionServiceServer(clusterServer *ClusterServer, options *RayJobSubmissionServiceServerOptions) *RayJobSubmissionServiceServer {
	zl := zerolog.New(os.Stdout).Level(zerolog.DebugLevel)
	return &RayJobSubmissionServiceServer{clusterServer: clusterServer, options: options, log: zerologr.New(&zl).WithName("jobsubmissionservice"), dashboardClientFunc: utils.GetRayDashboardClientFunc(context.Background(), nil, false)}
}

// Submit Ray job
func (s *RayJobSubmissionServiceServer) SubmitRayJob(ctx context.Context, req *api.SubmitRayJobRequest) (*api.SubmitRayJobReply, error) {
	s.log.Info("RayJobSubmissionService submit job")
	clusterRequest := api.GetClusterRequest{Name: req.Clustername, Namespace: req.Namespace}
	url, err := s.getRayClusterURL(ctx, &clusterRequest)
	if err != nil {
		return nil, err
	}
	rayDashboardClient, err := s.dashboardClientFunc(nil, *url)
	if err != nil {
		return nil, err
	}
	request := &utiltypes.RayJobRequest{Entrypoint: req.Jobsubmission.Entrypoint}
	if req.Jobsubmission.SubmissionId != "" {
		request.SubmissionId = req.Jobsubmission.SubmissionId
	}
	if len(req.Jobsubmission.Metadata) > 0 {
		request.Metadata = req.Jobsubmission.Metadata
	}
	if len(req.Jobsubmission.RuntimeEnv) > 0 {
		jsonData, err := yaml.YAMLToJSON([]byte(req.Jobsubmission.RuntimeEnv))
		if err != nil {
			return nil, err
		}
		re := make(map[string]any)
		err = json.Unmarshal(jsonData, &re)
		if err != nil {
			return nil, err
		}
		request.RuntimeEnv = re
	}
	if req.Jobsubmission.EntrypointNumCpus > 0 {
		request.NumCpus = req.Jobsubmission.EntrypointNumCpus
	}
	if req.Jobsubmission.EntrypointNumGpus > 0 {
		request.NumGpus = req.Jobsubmission.EntrypointNumGpus
	}
	if len(req.Jobsubmission.EntrypointResources) > 0 {
		for k, v := range req.Jobsubmission.EntrypointResources {
			f, err := strconv.ParseFloat(v, 32)
			if err != nil {
				return nil, err
			}
			request.Resources[k] = float32(f)
		}
	}

	sid, err := rayDashboardClient.SubmitJobReq(ctx, request)
	if err != nil {
		return nil, err
	}
	return &api.SubmitRayJobReply{SubmissionId: sid}, nil
}

// Get job details
func (s *RayJobSubmissionServiceServer) GetJobDetails(ctx context.Context, req *api.GetJobDetailsRequest) (*api.JobSubmissionInfo, error) {
	s.log.Info("RayJobSubmissionService get service")
	clusterRequest := api.GetClusterRequest{Name: req.Clustername, Namespace: req.Namespace}
	url, err := s.getRayClusterURL(ctx, &clusterRequest)
	if err != nil {
		return nil, err
	}
	rayDashboardClient, err := s.dashboardClientFunc(nil, *url)
	if err != nil {
		return nil, err
	}
	nodeInfo, err := rayDashboardClient.GetJobInfo(ctx, req.Submissionid)
	if err != nil {
		return nil, err
	}
	if nodeInfo == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "RayJob", Resource: "JobSubmission"}, req.Submissionid)
	}
	return convertNodeInfo(nodeInfo), nil
}

// Get Job log
func (s *RayJobSubmissionServiceServer) GetJobLog(ctx context.Context, req *api.GetJobLogRequest) (*api.GetJobLogReply, error) {
	s.log.Info("RayJobSubmissionService get job log")
	clusterRequest := api.GetClusterRequest{Name: req.Clustername, Namespace: req.Namespace}
	url, err := s.getRayClusterURL(ctx, &clusterRequest)
	if err != nil {
		return nil, err
	}
	rayDashboardClient, err := s.dashboardClientFunc(nil, *url)
	if err != nil {
		return nil, err
	}
	jlog, err := rayDashboardClient.GetJobLog(ctx, req.Submissionid)
	if err != nil {
		return nil, err
	}
	if jlog == nil {
		return nil, apierrors.NewNotFound(schema.GroupResource{Group: "RayJob", Resource: "JobSubmission"}, req.Submissionid)
	}
	return &api.GetJobLogReply{Log: *jlog}, nil
}

// List jobs
func (s *RayJobSubmissionServiceServer) ListJobDetails(ctx context.Context, req *api.ListJobDetailsRequest) (*api.ListJobSubmissionInfo, error) {
	s.log.Info("RayJobSubmissionService get jobs list")
	clusterRequest := api.GetClusterRequest{Name: req.Clustername, Namespace: req.Namespace}
	url, err := s.getRayClusterURL(ctx, &clusterRequest)
	if err != nil {
		return nil, err
	}
	rayDashboardClient, err := s.dashboardClientFunc(nil, *url)
	if err != nil {
		return nil, err
	}
	nodesInfo, err := rayDashboardClient.ListJobs(ctx)
	if err != nil {
		return nil, err
	}
	submissions := make([]*api.JobSubmissionInfo, 0)
	for _, nodeInfo := range *nodesInfo {
		submissions = append(submissions, convertNodeInfo(&nodeInfo))
	}
	return &api.ListJobSubmissionInfo{Submissions: submissions}, nil
}

// Stop job
func (s *RayJobSubmissionServiceServer) StopRayJob(ctx context.Context, req *api.StopRayJobSubmissionRequest) (*emptypb.Empty, error) {
	s.log.Info("RayJobSubmissionService stop job")
	clusterRequest := api.GetClusterRequest{Name: req.Clustername, Namespace: req.Namespace}
	url, err := s.getRayClusterURL(ctx, &clusterRequest)
	if err != nil {
		return nil, err
	}
	rayDashboardClient, err := s.dashboardClientFunc(nil, *url)
	if err != nil {
		return nil, err
	}
	err = rayDashboardClient.StopJob(ctx, req.Submissionid)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// Delete Job
func (s *RayJobSubmissionServiceServer) DeleteRayJob(ctx context.Context, req *api.DeleteRayJobSubmissionRequest) (*emptypb.Empty, error) {
	s.log.Info("RayJobSubmissionService delete job")
	clusterRequest := api.GetClusterRequest{Name: req.Clustername, Namespace: req.Namespace}
	url, err := s.getRayClusterURL(ctx, &clusterRequest)
	if err != nil {
		return nil, err
	}
	rayDashboardClient, err := s.dashboardClientFunc(nil, *url)
	if err != nil {
		return nil, err
	}
	err = rayDashboardClient.DeleteJob(ctx, req.Submissionid)
	if err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

// Internal method to get cluster for job operation
func (s *RayJobSubmissionServiceServer) getRayClusterURL(ctx context.Context, request *api.GetClusterRequest) (*string, error) {
	cls, err := s.clusterServer.GetCluster(ctx, request)
	if err != nil {
		return nil, err
	}
	if strings.ToLower(cls.ClusterState) != "ready" {
		return nil, errors.New("cluster is not ready")
	}
	// We are hardcoding port to the default value - 8265, as API server does not allow to modify it
	url := request.Name + "-head-svc." + request.Namespace + ".svc.cluster.local:8265"
	return &url, nil
}

// Internal method to convert RayJobInfo to JobSubmissionInfo
func convertNodeInfo(info *utiltypes.RayJobInfo) *api.JobSubmissionInfo {
	jsi := api.JobSubmissionInfo{
		Entrypoint:   info.Entrypoint,
		JobId:        info.JobId,
		SubmissionId: info.SubmissionId,
		Status:       string(info.JobStatus),
		Message:      info.Message,
		StartTime:    info.StartTime,
		EndTime:      info.EndTime,
	}
	if info.ErrorType != nil {
		jsi.ErrorType = *info.ErrorType
	}
	if len(info.Metadata) > 0 {
		jsi.Metadata = info.Metadata
	}
	if len(info.RuntimeEnv) > 0 {
		re := make(map[string]string)
		for key, value := range info.RuntimeEnv {
			re[key] = fmt.Sprintf("%v", value)
		}
		jsi.RuntimeEnv = re
	}
	return &jsi
}
