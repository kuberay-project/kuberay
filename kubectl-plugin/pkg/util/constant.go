package util

const (
	RayVersion = "2.52.0"
	RayImage   = "rayproject/ray:" + RayVersion

	RayClusterLabelKey   = "ray.io/cluster"
	RayIsRayNodeLabelKey = "ray.io/is-ray-node"
	RayNodeGroupLabelKey = "ray.io/group"
	RayNodeTypeLabelKey  = "ray.io/node-type"

	ResourceNvidiaGPU = "nvidia.com/gpu"
	ResourceGoogleTPU = "google.com/tpu"

	FieldManager = "ray-kubectl-plugin"

	// NodeSelector
	NodeSelectorGKETPUAccelerator = "cloud.google.com/gke-tpu-accelerator"
	NodeSelectorGKETPUTopology    = "cloud.google.com/gke-tpu-topology"

	DefaultHeadCPU                = "2"
	DefaultHeadMemory             = "4Gi"
	DefaultHeadGPU                = "0"
	DefaultHeadEphemeralStorage   = ""
	DefaultWorkerReplicas         = int32(1)
	DefaultWorkerCPU              = "2"
	DefaultWorkerMemory           = "4Gi"
	DefaultWorkerGPU              = "0"
	DefaultWorkerTPU              = "0"
	DefaultWorkerEphemeralStorage = ""
	DefaultNumOfHosts             = 1
)
