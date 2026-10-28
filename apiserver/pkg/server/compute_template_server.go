package server

import (
	"context"
	"fmt"

	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/ray-project/kuberay/apiserver/pkg/manager"
	"github.com/ray-project/kuberay/apiserver/pkg/model"
	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
)

type ComputeTemplateServerOptions struct {
	CollectMetrics bool
}

// implements `type ComputeTemplateServiceServer interface` in runtime_grpc.pb.go
// ComputeTemplateServer is the server API for ClusterRuntimeService.
type ComputeTemplateServer struct {
	api.UnimplementedComputeTemplateServiceServer
	resourceManager *manager.ResourceManager
	options         *ComputeTemplateServerOptions
}

func (s *ComputeTemplateServer) CreateComputeTemplate(ctx context.Context, request *api.CreateComputeTemplateRequest) (*api.ComputeTemplate, error) {
	if err := ValidateCreateComputeTemplateRequest(request); err != nil {
		return nil, util.Wrap(err, "Validate compute template runtime request failed.")
	}

	// use the namespace in the request to override the namespace in the compute template definition
	request.ComputeTemplate.Namespace = request.Namespace

	runtime, err := s.resourceManager.CreateComputeTemplate(ctx, request.ComputeTemplate)
	if err != nil {
		return nil, util.Wrap(err, "Create compute template failed.")
	}

	return model.FromKubeToAPIComputeTemplate(runtime), nil
}

func (s *ComputeTemplateServer) GetComputeTemplate(ctx context.Context, request *api.GetComputeTemplateRequest) (*api.ComputeTemplate, error) {
	if request.Name == "" {
		return nil, util.NewInvalidInputError("Compute template name is empty. Please specify a valid value.")
	}

	if request.Namespace == "" {
		return nil, util.NewInvalidInputError("Namespace is empty. Please specify a valid value.")
	}

	runtime, err := s.resourceManager.GetComputeTemplate(ctx, request.Name, request.Namespace)
	if err != nil {
		return nil, util.Wrap(err, "Get compute template failed.")
	}

	return model.FromKubeToAPIComputeTemplate(runtime), nil
}

func (s *ComputeTemplateServer) ListComputeTemplates(ctx context.Context, request *api.ListComputeTemplatesRequest) (*api.ListComputeTemplatesResponse, error) {
	if request.Namespace == "" {
		return nil, util.NewInvalidInputError("Namespace is empty. Please specify a valid value.")
	}

	runtimes, err := s.resourceManager.ListComputeTemplates(ctx, request.Namespace)
	if err != nil {
		return nil, util.Wrap(err, fmt.Sprintf("List compute templates in namespace %s failed.", request.Namespace))
	}

	return &api.ListComputeTemplatesResponse{
		ComputeTemplates: model.FromKubeToAPIComputeTemplates(runtimes),
	}, nil
}

func (s *ComputeTemplateServer) ListAllComputeTemplates(ctx context.Context, _ *api.ListAllComputeTemplatesRequest) (*api.ListAllComputeTemplatesResponse, error) {
	runtimes, err := s.resourceManager.ListAllComputeTemplates(ctx)
	if err != nil {
		return nil, util.Wrap(err, "List all compute templates from all namespaces failed.")
	}

	return &api.ListAllComputeTemplatesResponse{
		ComputeTemplates: model.FromKubeToAPIComputeTemplates(runtimes),
	}, nil
}

func (s *ComputeTemplateServer) DeleteComputeTemplate(ctx context.Context, request *api.DeleteComputeTemplateRequest) (*emptypb.Empty, error) {
	if request.Name == "" {
		return nil, util.NewInvalidInputError("Compute template name is empty. Please specify a valid value.")
	}

	if request.Namespace == "" {
		return nil, util.NewInvalidInputError("Namespace is empty. Please specify a valid value.")
	}

	if err := s.resourceManager.DeleteComputeTemplate(ctx, request.Name, request.Namespace); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func ValidateCreateComputeTemplateRequest(request *api.CreateComputeTemplateRequest) error {
	if request.Namespace == "" {
		return util.NewInvalidInputError("Namespace is empty. Please specify a valid value.")
	}

	if request.Namespace != request.ComputeTemplate.Namespace {
		return util.NewInvalidInputError("The namespace in the request is different from the namespace defined in the compute template.")
	}

	if request.ComputeTemplate.Name == "" {
		return util.NewInvalidInputError("Compute template name is empty. Please specify a valid value.")
	}

	if request.ComputeTemplate.Cpu == 0 {
		return util.NewInvalidInputError("Cpu amount is zero. Please specify a valid value.")
	}

	if request.ComputeTemplate.Memory == 0 {
		return util.NewInvalidInputError("Memory amount is zero. Please specify a valid value.")
	}

	return nil
}

func NewComputeTemplateServer(resourceManager *manager.ResourceManager, options *ComputeTemplateServerOptions) *ComputeTemplateServer {
	return &ComputeTemplateServer{resourceManager: resourceManager, options: options}
}
