package server

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ray-project/kuberay/apiserver/pkg/manager"
	"github.com/ray-project/kuberay/apiserver/pkg/model"
	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
)

type JobServerOptions struct {
	CollectMetrics bool
}

// implements `type RayJobServiceServer interface` in job_grpc.pb.go
// RayJobServer is the server API for RayJobServer service.

func NewRayJobServer(resourceManager *manager.ResourceManager, options *JobServerOptions) *RayJobServer {
	return &RayJobServer{resourceManager: resourceManager, options: options}
}

type RayJobServer struct {
	api.UnimplementedRayJobServiceServer
	resourceManager *manager.ResourceManager
	options         *JobServerOptions
}

// Creates a new Ray Job.
func (s *RayJobServer) CreateRayJob(ctx context.Context, request *api.CreateRayJobRequest) (*api.RayJob, error) {
	if err := ValidateCreateJobRequest(request); err != nil {
		return nil, util.Wrap(err, "Validate job request failed.")
	}

	// use the namespace in the request to override the namespace in the job definition
	request.Job.Namespace = request.Namespace

	job, err := s.resourceManager.CreateJob(ctx, request.Job)
	if err != nil {
		return nil, util.Wrap(err, "Create Job failed.")
	}

	return model.FromCrdToAPIJob(job), nil
}

// Finds a specific Job by job name.
func (s *RayJobServer) GetRayJob(ctx context.Context, request *api.GetRayJobRequest) (*api.RayJob, error) {
	if request.Name == "" {
		return nil, util.NewInvalidInputError("job name is empty. Please specify a valid value.")
	}

	if request.Namespace == "" {
		return nil, util.NewInvalidInputError("job namespace is empty. Please specify a valid value.")
	}

	job, err := s.resourceManager.GetJob(ctx, request.Name, request.Namespace)
	if err != nil {
		return nil, util.Wrap(err, "Get cluster failed.")
	}

	return model.FromCrdToAPIJob(job), nil
}

// Finds all Jobs in a given namespace.
func (s *RayJobServer) ListRayJobs(ctx context.Context, request *api.ListRayJobsRequest) (*api.ListRayJobsResponse, error) {
	if request.Namespace == "" {
		return nil, util.NewInvalidInputError("job namespace is empty. Please specify a valid value.")
	}

	jobs, continueToken, err := s.resourceManager.ListJobs(ctx, request.Namespace, request.Continue, request.Limit)
	if err != nil {
		return nil, util.Wrap(err, "List jobs failed.")
	}

	return &api.ListRayJobsResponse{
		Jobs:     model.FromCrdToAPIJobs(jobs),
		Continue: continueToken,
	}, nil
}

// Finds all Jobs in all namespaces.
func (s *RayJobServer) ListAllRayJobs(ctx context.Context, request *api.ListAllRayJobsRequest) (*api.ListAllRayJobsResponse, error) {
	// Leave the namespace empty to list all jobs in all namespaces.
	jobs, continueToken, err := s.resourceManager.ListJobs(ctx, "" /*namespace*/, request.Continue, request.Limit)
	if err != nil {
		return nil, util.Wrap(err, "List jobs failed.")
	}

	return &api.ListAllRayJobsResponse{
		Jobs:     model.FromCrdToAPIJobs(jobs),
		Continue: continueToken,
	}, nil
}

// Deletes an Job
func (s *RayJobServer) DeleteRayJob(ctx context.Context, request *api.DeleteRayJobRequest) (*emptypb.Empty, error) {
	if request.Name == "" {
		return nil, util.NewInvalidInputError("job name is empty. Please specify a valid value.")
	}

	if request.Namespace == "" {
		return nil, util.NewInvalidInputError("job namespace is empty. Please specify a valid value.")
	}

	if err := s.resourceManager.DeleteJob(ctx, request.Name, request.Namespace); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func ValidateCreateJobRequest(request *api.CreateRayJobRequest) error {
	if request.Namespace == "" {
		return util.NewInvalidInputError("Namespace is empty. Please specify a valid value.")
	}

	if request.Namespace != request.Job.Namespace {
		return util.NewInvalidInputError("The namespace in the request is different from the namespace in the job definition.")
	}

	if request.Job.Name == "" {
		return util.NewInvalidInputError("Job name is empty. Please specify a valid value.")
	}

	if request.Job.User == "" {
		return util.NewInvalidInputError("User who create the job is empty. Please specify a valid value.")
	}

	if len(request.Job.ClusterSelector) != 0 {
		return nil
	}

	return ValidateClusterSpec(request.Job.ClusterSpec)
}
