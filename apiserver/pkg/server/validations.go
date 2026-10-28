package server

import (
	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
)

// ValidateClusterSpec validates that the *api.ClusterSpec is not nil and
// has all the required fields
func ValidateClusterSpec(clusterSpec *api.ClusterSpec) error {
	if clusterSpec == nil {
		return util.NewInvalidInputError("A ClusterSpec object is required. Please specify one.")
	}
	if clusterSpec.HeadGroupSpec == nil {
		return util.NewInvalidInputError("Cluster Spec Object requires HeadGroupSpec to be populated. Please specify one.")
	}
	if len(clusterSpec.HeadGroupSpec.ComputeTemplate) == 0 {
		return util.NewInvalidInputError("HeadGroupSpec compute template is empty. Please specify a valid value.")
	}
	if len(clusterSpec.HeadGroupSpec.RayStartParams) == 0 {
		return util.NewInvalidInputError("HeadGroupSpec RayStartParams is empty. Please specify values.")
	}
	if len(clusterSpec.HeadGroupSpec.ImagePullPolicy) > 0 &&
		clusterSpec.HeadGroupSpec.ImagePullPolicy != "Always" && clusterSpec.HeadGroupSpec.ImagePullPolicy != "IfNotPresent" {
		return util.NewInvalidInputError("HeadGroupSpec unsupported value for Image pull policy. Please specify Always or IfNotPresent")
	}

	for index, spec := range clusterSpec.WorkerGroupSpec {
		if len(spec.GroupName) == 0 {
			return util.NewInvalidInputError("WorkerNodeSpec %d group name is empty. Please specify a valid value.", index)
		}
		if len(spec.ComputeTemplate) == 0 {
			return util.NewInvalidInputError("WorkerNodeSpec %d compute template is empty. Please specify a valid value.", index)
		}
		if spec.MaxReplicas == 0 {
			return util.NewInvalidInputError("WorkerNodeSpec %d MaxReplicas can not be 0. Please specify a valid value.", index)
		}
		if spec.MinReplicas > spec.MaxReplicas {
			return util.NewInvalidInputError("WorkerNodeSpec %d MinReplica > MaxReplicas. Please specify a valid value.", index)
		}
		if len(spec.ImagePullPolicy) > 0 && spec.ImagePullPolicy != "Always" && spec.ImagePullPolicy != "IfNotPresent" {
			return util.NewInvalidInputError("Worker GroupSpec unsupported value for Image pull policy. Please specify Always or IfNotPresent")
		}
	}
	return nil
}
