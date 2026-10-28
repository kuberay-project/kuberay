package server

import (
	"context"

	"google.golang.org/protobuf/types/known/emptypb"
	eventsv1 "k8s.io/api/events/v1"
	klog "k8s.io/klog/v2"

	"github.com/ray-project/kuberay/apiserver/pkg/manager"
	"github.com/ray-project/kuberay/apiserver/pkg/model"
	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
)

type ServiceServerOptions struct {
	CollectMetrics bool
}

// implements `type RayServeServiceServer interface` in serve_grpc.pb.go
// RayServiceServer is the server API for RayServeService service.
type RayServiceServer struct {
	api.UnimplementedRayServeServiceServer
	resourceManager *manager.ResourceManager
	options         *ServiceServerOptions
}

func NewRayServiceServer(resourceManager *manager.ResourceManager, options *ServiceServerOptions) *RayServiceServer {
	return &RayServiceServer{resourceManager: resourceManager, options: options}
}

// Create a new Ray Service
func (s *RayServiceServer) CreateRayService(ctx context.Context, request *api.CreateRayServiceRequest) (*api.RayService, error) {
	if err := ValidateCreateServiceRequest(request); err != nil {
		return nil, util.Wrap(err, "Validate create service request failed.")
	}

	request.Service.Namespace = request.Namespace

	rayService, err := s.resourceManager.CreateService(ctx, request.Service)
	if err != nil {
		return nil, util.Wrap(err, "Create ray service failed.")
	}
	events, err := s.resourceManager.GetServiceEvents(ctx, *rayService)
	if err != nil {
		klog.Warningf("failed to get rayService's event, service: %s/%s, err: %v", rayService.Namespace, rayService.Name, err)
	}
	return model.FromCrdToAPIService(rayService, events), nil
}

func (s *RayServiceServer) UpdateRayService(ctx context.Context, request *api.UpdateRayServiceRequest) (*api.RayService, error) {
	if err := ValidateUpdateServiceRequest(request); err != nil {
		return nil, util.Wrap(err, "Validate update service request failed.")
	}
	request.Service.Namespace = request.Namespace

	rayService, err := s.resourceManager.UpdateRayService(ctx, request.Service)
	if err != nil {
		return nil, util.Wrap(err, "Update ray service failed.")
	}
	events, err := s.resourceManager.GetServiceEvents(ctx, *rayService)
	if err != nil {
		klog.Warningf("failed to get rayService's event, service: %s/%s, err: %v", rayService.Namespace, rayService.Name, err)
	}
	return model.FromCrdToAPIService(rayService, events), nil
}

func (s *RayServiceServer) GetRayService(ctx context.Context, request *api.GetRayServiceRequest) (*api.RayService, error) {
	if request.Name == "" {
		return nil, util.NewInvalidInputError("ray service name is empty. Please specify a valid value.")
	}

	if request.Namespace == "" {
		return nil, util.NewInvalidInputError("ray service namespace is empty. Please specify a valid value.")
	}
	service, err := s.resourceManager.GetService(ctx, request.Name, request.Namespace)
	if err != nil {
		return nil, util.Wrap(err, "get ray service failed")
	}
	events, err := s.resourceManager.GetServiceEvents(ctx, *service)
	if err != nil {
		klog.Warningf("failed to get rayService's event, service: %s/%s, err: %v", service.Namespace, service.Name, err)
	}
	return model.FromCrdToAPIService(service, events), nil
}

func (s *RayServiceServer) ListRayServices(ctx context.Context, request *api.ListRayServicesRequest) (*api.ListRayServicesResponse, error) {
	if request.Namespace == "" {
		return nil, util.NewInvalidInputError("ray service namespace is empty. Please specify a valid value.")
	}
	services, nextPageToken, err := s.resourceManager.ListServices(ctx, request.Namespace, request.PageToken, request.PageSize)
	if err != nil {
		return nil, util.Wrap(err, "failed to list rayservice.")
	}

	serviceEventMap := make(map[string][]eventsv1.Event)
	for _, service := range services {
		serviceEvents, err := s.resourceManager.GetServiceEvents(ctx, *service)
		if err != nil {
			klog.Warningf("Failed to get cluster's event, cluster: %s/%s, err: %v", service.Namespace, service.Name, err)
			continue
		}
		serviceEventMap[service.Name] = serviceEvents
	}
	return &api.ListRayServicesResponse{
		Services:      model.FromCrdToAPIServices(services, serviceEventMap),
		NextPageToken: nextPageToken,
	}, nil
}

func (s *RayServiceServer) ListAllRayServices(ctx context.Context, request *api.ListAllRayServicesRequest) (*api.ListAllRayServicesResponse, error) {
	services, nextPageToken, err := s.resourceManager.ListServices(ctx, "" /*namespace*/, request.PageToken, request.PageSize)
	if err != nil {
		return nil, util.Wrap(err, "list all services failed.")
	}
	serviceEventMap := make(map[string][]eventsv1.Event)
	for _, service := range services {
		serviceEvents, err := s.resourceManager.GetServiceEvents(ctx, *service)
		if err != nil {
			klog.Warningf("Failed to get cluster's event, cluster: %s/%s, err: %v", service.Namespace, service.Name, err)
			continue
		}
		serviceEventMap[service.Name] = serviceEvents
	}
	return &api.ListAllRayServicesResponse{
		Services:      model.FromCrdToAPIServices(services, serviceEventMap),
		NextPageToken: nextPageToken,
	}, nil
}

func (s *RayServiceServer) DeleteRayService(ctx context.Context, request *api.DeleteRayServiceRequest) (*emptypb.Empty, error) {
	if request.Name == "" {
		return nil, util.NewInvalidInputError("ray service name is empty. Please specify a valid value.")
	}

	if request.Namespace == "" {
		return nil, util.NewInvalidInputError("ray service namespace is empty. Please specify a valid value.")
	}
	if err := s.resourceManager.DeleteService(ctx, request.Name, request.Namespace); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

func ValidateCreateServiceRequest(request *api.CreateRayServiceRequest) error {
	if request == nil {
		return util.NewInvalidInputError("A non nill request is expected")
	}
	if request.Namespace == "" {
		return util.NewInvalidInputError("Namespace is empty. Please specify a valid value.")
	}

	if request.Service == nil {
		return util.NewInvalidInputError("Service is empty, please input a valid payload.")
	}

	if request.Namespace != request.Service.Namespace {
		return util.NewInvalidInputError("The namespace in the request is different from the namespace in the service definition.")
	}

	if request.Service.Name == "" {
		return util.NewInvalidInputError("Service name is empty. Please specify a valid value.")
	}

	if request.Service.User == "" {
		return util.NewInvalidInputError("User who created the Service is empty. Please specify a valid value.")
	}

	return ValidateClusterSpec(request.Service.ClusterSpec)
}

func ValidateUpdateServiceRequest(request *api.UpdateRayServiceRequest) error {
	if request.Name == "" {
		return util.NewInvalidInputError("Name is empty. Please specify a valid value.")
	}
	if request.Namespace == "" {
		return util.NewInvalidInputError("Namespace is empty. Please specify a valid value.")
	}

	if request.Service == nil {
		return util.NewInvalidInputError("Service is empty, please input a valid payload.")
	}

	if request.Namespace != request.Service.Namespace {
		return util.NewInvalidInputError("The namespace in the request is different from the namespace in the service definition.")
	}

	if request.Service.Name == "" {
		return util.NewInvalidInputError("Service name is empty. Please specify a valid value.")
	}

	if request.Service.User == "" {
		return util.NewInvalidInputError("User who updated the Service is empty. Please specify a valid value.")
	}

	return ValidateClusterSpec(request.Service.ClusterSpec)
}
