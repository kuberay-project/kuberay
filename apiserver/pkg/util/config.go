package util

import "time"

// ClientOptions contains configuration needed to create a Kubernetes client
type ClientOptions struct {
	QPS   float32
	Burst int
}

// TODO: this needs to be revised.
const (
	// Label keys
	RayClusterNameLabelKey            = "ray.io/cluster-name"
	RayClusterUserLabelKey            = "ray.io/user"
	RayClusterVersionLabelKey         = "ray.io/version"
	RayClusterEnvironmentLabelKey     = "ray.io/environment"
	KubernetesApplicationNameLabelKey = "app.kubernetes.io/name"
	KubernetesManagedByLabelKey       = "app.kubernetes.io/managed-by"
	KubernetesComponentLabelKey       = "app.kubernetes.io/component"

	// Annotation keys
	// Role level
	RayClusterComputeTemplateAnnotationKey = "ray.io/compute-template"
	RayClusterImageAnnotationKey           = "ray.io/compute-image"

	RayClusterDefaultImageRepository = "rayproject/ray"
)

const (
	// The application name
	ApplicationName = "kuberay"

	// The component name for apiserver
	ComponentName = "kuberay-apiserver"

	// The curl container name for apiserver
	CurlContainerName = "apiserver-curl"

	// Timeout for apiserver gRPC server
	GRPCServerDefaultTimeout = 60 * time.Second
)
