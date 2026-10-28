package util

import (
	"errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

type RayService struct {
	*rayv1api.RayService
}

func (s *RayService) Get() *rayv1api.RayService {
	return s.RayService
}

func NewRayService(apiService *api.RayService, computeTemplateMap map[string]*api.ComputeTemplate) (*RayService, error) {
	// Build the spec
	spec, err := buildRayServiceSpec(apiService, computeTemplateMap)
	if err != nil {
		return nil, err
	}
	// Build Ray service
	rayService := &rayv1api.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:        apiService.Name,
			Namespace:   apiService.Namespace,
			Labels:      buildRayServiceLabels(apiService),
			Annotations: buildRayServiceAnnotations(apiService),
		},
		Spec: *spec,
	}
	return &RayService{rayService}, nil
}

func buildRayServiceLabels(apiService *api.RayService) map[string]string {
	labels := map[string]string{}
	labels[RayClusterUserLabelKey] = apiService.User
	labels[KubernetesApplicationNameLabelKey] = ApplicationName
	labels[KubernetesManagedByLabelKey] = ComponentName
	return labels
}

func buildRayServiceAnnotations(_ *api.RayService) map[string]string {
	annotations := map[string]string{}
	// TODO: Add optional annotations
	return annotations
}

func buildRayServiceSpec(apiService *api.RayService, computeTemplateMap map[string]*api.ComputeTemplate) (*rayv1api.RayServiceSpec, error) {
	// Ensure that at least one and only one serve config (V1 or V2) defined
	if apiService.ServeConfig_V2 == "" {
		// Serve configuration is not defined
		return nil, errors.New("serve configuration is not defined")
	}

	// generate Ray cluster spec and buid cluster
	newRayClusterSpec, err := buildRayClusterSpec(apiService.Version, nil, apiService.ClusterSpec, computeTemplateMap, true)
	if err != nil {
		return nil, err
	}
	var serviceUnhealthySecondThreshold *int32
	if apiService.ServiceUnhealthySecondThreshold > 0 {
		serviceUnhealthySecondThreshold = &apiService.ServiceUnhealthySecondThreshold
	} else {
		serviceUnhealthySecondThreshold = nil
	}
	var deploymentUnhealthySecondThreshold *int32
	if apiService.DeploymentUnhealthySecondThreshold > 0 {
		deploymentUnhealthySecondThreshold = &apiService.DeploymentUnhealthySecondThreshold
	} else {
		deploymentUnhealthySecondThreshold = nil
	}
	// V2 definition
	return &rayv1api.RayServiceSpec{
		ServeConfigV2:                      apiService.ServeConfig_V2,
		RayClusterSpec:                     *newRayClusterSpec,
		ServiceUnhealthySecondThreshold:    serviceUnhealthySecondThreshold,
		DeploymentUnhealthySecondThreshold: deploymentUnhealthySecondThreshold,
	}, nil
}

func UpdateRayServiceWorkerGroupSpecs(updateSpecs []*api.WorkerGroupUpdateSpec, workerGroupSpecs []rayv1api.WorkerGroupSpec) []rayv1api.WorkerGroupSpec {
	specMap := map[string]*api.WorkerGroupUpdateSpec{}
	for _, spec := range updateSpecs {
		if spec != nil {
			specMap[spec.GroupName] = spec
		}
	}
	for i, spec := range workerGroupSpecs {
		if updateSpec, ok := specMap[spec.GroupName]; ok {
			newSpec := updateWorkerGroupSpec(updateSpec, spec)
			workerGroupSpecs[i] = newSpec
		}
	}
	return workerGroupSpecs
}

func updateWorkerGroupSpec(updateSpec *api.WorkerGroupUpdateSpec, workerGroupSpec rayv1api.WorkerGroupSpec) rayv1api.WorkerGroupSpec {
	replicas := updateSpec.Replicas
	minReplicas := updateSpec.MinReplicas
	maxReplicas := updateSpec.MaxReplicas

	workerGroupSpec.Replicas = &replicas
	workerGroupSpec.MinReplicas = &minReplicas
	workerGroupSpec.MaxReplicas = &maxReplicas
	return workerGroupSpec
}
