package generation

import (
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1 "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
)

const (
	volumeName       = "cluster-storage"
	gcsFuseCSIDriver = "gcsfuse.csi.storage.gke.io"
)

type RayClusterConfig struct {
	Namespace      *string           `yaml:"namespace,omitempty"`
	Name           *string           `yaml:"name,omitempty"`
	Labels         map[string]string `yaml:"labels,omitempty"`
	Annotations    map[string]string `yaml:"annotations,omitempty"`
	RayVersion     *string           `yaml:"ray-version,omitempty"`
	Image          *string           `yaml:"image,omitempty"`
	ServiceAccount *string           `yaml:"service-account,omitempty"`
	Head           *Head             `yaml:"head,omitempty"`
	GKE            *GKE              `yaml:"gke,omitempty"`
	Autoscaler     *Autoscaler       `yaml:"autoscaler,omitempty"`
	WorkerGroups   []WorkerGroup     `yaml:"worker-groups,omitempty"`
}

type Head struct {
	CPU              *string           `yaml:"cpu,omitempty"`
	GPU              *string           `yaml:"gpu,omitempty"`
	Memory           *string           `yaml:"memory,omitempty"`
	EphemeralStorage *string           `yaml:"ephemeral-storage,omitempty"`
	RayStartParams   map[string]string `yaml:"ray-start-params,omitempty"`
	NodeSelectors    map[string]string `yaml:"node-selectors,omitempty"`
}

type WorkerGroup struct {
	Name             *string           `yaml:"name,omitempty"`
	CPU              *string           `yaml:"cpu,omitempty"`
	GPU              *string           `yaml:"gpu,omitempty"`
	TPU              *string           `yaml:"tpu,omitempty"`
	NumOfHosts       *int32            `yaml:"num-of-hosts,omitempty"`
	Memory           *string           `yaml:"memory,omitempty"`
	EphemeralStorage *string           `yaml:"ephemeral-storage,omitempty"`
	RayStartParams   map[string]string `yaml:"ray-start-params,omitempty"`
	NodeSelectors    map[string]string `yaml:"node-selectors,omitempty"`
	Replicas         int32             `yaml:"replicas"`
}

type AutoscalerVersion string

const (
	AutoscalerV1 AutoscalerVersion = "v1"
	AutoscalerV2 AutoscalerVersion = "v2"
)

// String is used both by fmt.Print and by Cobra in help text
func (e *AutoscalerVersion) String() string {
	return string(*e)
}

// Set must have pointer receiver so it doesn't change the value of a copy
func (e *AutoscalerVersion) Set(v string) error {
	val := strings.ToLower(v)

	switch val {
	case string(AutoscalerV1), string(AutoscalerV2):
		*e = AutoscalerVersion(val)
		return nil
	default:
		return fmt.Errorf("must be one of %q or %q", AutoscalerV1, AutoscalerV2)
	}
}

// Type is only used in help text
func (e *AutoscalerVersion) Type() string {
	return "enum"
}

type Autoscaler struct {
	Version AutoscalerVersion `yaml:"version,omitempty"`
}

// GKE is a struct that contains the GKE specific configuration
type GKE struct {
	GCSFuse *GCSFuse `yaml:"gcsfuse,omitempty"`
}

type GCSFuse struct {
	MountOptions                   *string           `yaml:"mount-options,omitempty"`
	DisableMetrics                 *bool             `yaml:"disable-metrics,omitempty"`
	GCSFuseMetadataPrefetchOnMount *bool             `yaml:"gcsfuse-metadata-prefetch-on-mount,omitempty"`
	SkipCSIBucketAccessCheck       *bool             `yaml:"skip-csi-bucket-access-check,omitempty"`
	Resources                      *GCSFuseResources `yaml:"resources,omitempty"`
	BucketName                     string            `yaml:"bucket-name"`
	MountPath                      string            `yaml:"mount-path"`
}

type GCSFuseResources struct {
	CPU              *string `yaml:"cpu,omitempty"`
	Memory           *string `yaml:"memory,omitempty"`
	EphemeralStorage *string `yaml:"ephemeral-storage,omitempty"`
}

type RayJobYamlObject struct {
	RayJobName     string
	Namespace      string
	SubmissionMode string
	Entrypoint     string
	RayClusterConfig
	TTLSecondsAfterFinished  int32
	ShutdownAfterJobFinishes bool
}

func (rayClusterConfig *RayClusterConfig) GenerateRayClusterApplyConfig() *rayv1ac.RayClusterApplyConfiguration {
	rayClusterApplyConfig := rayv1ac.RayCluster(*rayClusterConfig.Name, *rayClusterConfig.Namespace).
		WithLabels(rayClusterConfig.Labels).
		WithAnnotations(rayClusterConfig.Annotations).
		WithSpec(rayClusterConfig.generateRayClusterSpec())

	return rayClusterApplyConfig
}

func (rayJobObject *RayJobYamlObject) GenerateRayJobApplyConfig() *rayv1ac.RayJobApplyConfiguration {
	rayJobApplyConfig := rayv1ac.RayJob(rayJobObject.RayJobName, rayJobObject.Namespace).
		WithSpec(rayv1ac.RayJobSpec().
			WithSubmissionMode(rayv1.JobSubmissionMode(rayJobObject.SubmissionMode)).
			WithEntrypoint(rayJobObject.Entrypoint).
			WithTTLSecondsAfterFinished(rayJobObject.TTLSecondsAfterFinished).
			WithShutdownAfterJobFinishes(rayJobObject.ShutdownAfterJobFinishes).
			WithRayClusterSpec(rayJobObject.generateRayClusterSpec()))

	return rayJobApplyConfig
}

// generateRequestResources returns a corev1.ResourceList with the given CPU, memory, ephemeral storage, GPU, and TPU values for only resource requests
func generateRequestResources(cpu, memory, ephemeralStorage, gpu, tpu *string) corev1.ResourceList {
	resources := corev1.ResourceList{}

	if cpu != nil && *cpu != "" {
		cpuResource := resource.MustParse(*cpu)
		if !cpuResource.IsZero() {
			resources[corev1.ResourceCPU] = cpuResource
		}
	}

	if memory != nil && *memory != "" {
		memoryResource := resource.MustParse(*memory)
		if !memoryResource.IsZero() {
			resources[corev1.ResourceMemory] = memoryResource
		}
	}

	if ephemeralStorage != nil && *ephemeralStorage != "" {
		ephemeralStorageResource := resource.MustParse(*ephemeralStorage)
		if !ephemeralStorageResource.IsZero() {
			resources[corev1.ResourceEphemeralStorage] = ephemeralStorageResource
		}
	}

	if gpu != nil && *gpu != "" {
		gpuResource := resource.MustParse(*gpu)
		if !gpuResource.IsZero() {
			resources[corev1.ResourceName(util.ResourceNvidiaGPU)] = gpuResource
		}
	}

	if tpu != nil && *tpu != "" {
		tpuResource := resource.MustParse(*tpu)
		if !tpuResource.IsZero() {
			resources[corev1.ResourceName(util.ResourceGoogleTPU)] = tpuResource
		}
	}

	return resources
}

// generateLimitResources returns a corev1.ResourceList with the given memory, ephemeral storage, GPU, and TPU values for only resource limits
func generateLimitResources(memory, ephemeralStorage, gpu, tpu *string) corev1.ResourceList {
	resources := corev1.ResourceList{}

	if memory != nil && *memory != "" {
		memoryResource := resource.MustParse(*memory)
		if !memoryResource.IsZero() {
			resources[corev1.ResourceMemory] = memoryResource
		}
	}

	if ephemeralStorage != nil && *ephemeralStorage != "" {
		ephemeralStorageResource := resource.MustParse(*ephemeralStorage)
		if !ephemeralStorageResource.IsZero() {
			resources[corev1.ResourceEphemeralStorage] = ephemeralStorageResource
		}
	}

	if gpu != nil && *gpu != "" {
		gpuResource := resource.MustParse(*gpu)
		if !gpuResource.IsZero() {
			resources[corev1.ResourceName(util.ResourceNvidiaGPU)] = gpuResource
		}
	}

	if tpu != nil && *tpu != "" {
		tpuResource := resource.MustParse(*tpu)
		if !tpuResource.IsZero() {
			resources[corev1.ResourceName(util.ResourceGoogleTPU)] = tpuResource
		}
	}

	return resources
}

func (rayClusterConfig *RayClusterConfig) generateRayClusterSpec() *rayv1ac.RayClusterSpecApplyConfiguration {
	headRequestResources := generateRequestResources(rayClusterConfig.Head.CPU, rayClusterConfig.Head.Memory, rayClusterConfig.Head.EphemeralStorage, rayClusterConfig.Head.GPU,
		// TPU is not used for head request resources
		nil)
	headLimitResources := generateLimitResources(rayClusterConfig.Head.Memory, rayClusterConfig.Head.EphemeralStorage, rayClusterConfig.Head.GPU,
		// TPU is not used for head limit resources
		nil)

	workerGroupSpecs := make([]*rayv1ac.WorkerGroupSpecApplyConfiguration, len(rayClusterConfig.WorkerGroups))
	for i, workerGroup := range rayClusterConfig.WorkerGroups {
		if workerGroup.Name == nil || *workerGroup.Name == "" {
			name := "default-group"
			// For backwards compatibility, if no name is specified, use "default-group"
			if i != 0 {
				name = fmt.Sprintf("default-group-%d", i)
			}
			workerGroup.Name = new(name)
		}

		workerRequestResources := generateRequestResources(workerGroup.CPU, workerGroup.Memory, workerGroup.EphemeralStorage, workerGroup.GPU, workerGroup.TPU)
		workerLimitResources := generateLimitResources(workerGroup.Memory, workerGroup.EphemeralStorage, workerGroup.GPU, workerGroup.TPU)

		workerGroupSpecs[i] = rayv1ac.WorkerGroupSpec().
			WithRayStartParams(workerGroup.RayStartParams).
			WithGroupName(*workerGroup.Name).
			WithReplicas(workerGroup.Replicas).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithSpec(corev1ac.PodSpec().
					WithNodeSelector(workerGroup.NodeSelectors).
					WithContainers(corev1ac.Container().
						WithName("ray-worker").
						WithImage(*rayClusterConfig.Image).
						WithResources(corev1ac.ResourceRequirements().
							WithRequests(workerRequestResources).
							WithLimits(workerLimitResources)))))

		if workerGroup.NumOfHosts != nil {
			workerGroupSpecs[i].WithNumOfHosts(*workerGroup.NumOfHosts)
		}

		if rayClusterConfig.ServiceAccount != nil && *rayClusterConfig.ServiceAccount != "" {
			workerGroupSpecs[i].Template.Spec.ServiceAccountName = new(*rayClusterConfig.ServiceAccount)
		}
	}

	rayClusterSpec := rayv1ac.RayClusterSpec().
		WithRayVersion(*rayClusterConfig.RayVersion).
		WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
			WithRayStartParams(rayClusterConfig.Head.RayStartParams).
			WithTemplate(corev1ac.PodTemplateSpec().
				WithSpec(corev1ac.PodSpec().
					WithNodeSelector(rayClusterConfig.Head.NodeSelectors).
					WithContainers(corev1ac.Container().
						WithName("ray-head").
						WithImage(*rayClusterConfig.Image).
						WithResources(corev1ac.ResourceRequirements().
							WithRequests(headRequestResources).
							WithLimits(headLimitResources)).
						WithPorts(corev1ac.ContainerPort().WithContainerPort(6379).WithName("gcs-server"),
							corev1ac.ContainerPort().WithContainerPort(8265).WithName("dashboard"),
							corev1ac.ContainerPort().WithContainerPort(10001).WithName("client")))))).
		WithWorkerGroupSpecs(workerGroupSpecs...)

	if rayClusterConfig.Autoscaler != nil && rayClusterConfig.Autoscaler.Version != "" {
		rayClusterSpec.WithEnableInTreeAutoscaling(true).
			WithAutoscalerOptions(rayv1ac.AutoscalerOptions().WithVersion(rayv1.AutoscalerVersion(rayClusterConfig.Autoscaler.Version)))
	}

	if rayClusterConfig.ServiceAccount != nil && *rayClusterConfig.ServiceAccount != "" {
		rayClusterSpec.HeadGroupSpec.Template.Spec.ServiceAccountName = new(*rayClusterConfig.ServiceAccount)
	}

	if rayClusterConfig.GKE != nil {
		setGCSFuseOptions(rayClusterSpec, rayClusterConfig.GKE.GCSFuse)
	}

	return rayClusterSpec
}

func setGCSFuseOptions(rayClusterSpec *rayv1ac.RayClusterSpecApplyConfiguration, gcsFuse *GCSFuse) {
	if gcsFuse == nil {
		return
	}

	volumeAttributes := getGCSFuseVolumeAttributes(gcsFuse)

	rayClusterSpec.HeadGroupSpec.Template.Spec.Volumes = append(rayClusterSpec.HeadGroupSpec.Template.Spec.Volumes, *corev1ac.Volume().
		WithName(volumeName).
		WithCSI(corev1ac.CSIVolumeSource().
			WithDriver(gcsFuseCSIDriver).
			WithVolumeAttributes(volumeAttributes)))

	rayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].VolumeMounts = append(rayClusterSpec.HeadGroupSpec.Template.Spec.Containers[0].VolumeMounts, *corev1ac.VolumeMount().
		WithName(volumeName).
		WithMountPath(gcsFuse.MountPath))

	for _, workerGroup := range rayClusterSpec.WorkerGroupSpecs {
		workerGroup.Template.Spec.Volumes = append(workerGroup.Template.Spec.Volumes, *corev1ac.Volume().
			WithName(volumeName).
			WithCSI(corev1ac.CSIVolumeSource().
				WithDriver(gcsFuseCSIDriver).
				WithVolumeAttributes(volumeAttributes)))

		workerGroup.Template.Spec.Containers[0].VolumeMounts = append(workerGroup.Template.Spec.Containers[0].VolumeMounts, *corev1ac.VolumeMount().
			WithName(volumeName).
			WithMountPath(gcsFuse.MountPath))
	}

	// If GCSFuse sidecar resources are specified, add as K8s annotations to all Pods
	extraAnnotations := make(map[string]string, 5)
	if gcsFuse.Resources != nil {
		if gcsFuse.Resources.CPU != nil && *gcsFuse.Resources.CPU != "" {
			extraAnnotations["gke-gcsfuse/cpu-request"] = *gcsFuse.Resources.CPU
		}

		if gcsFuse.Resources.Memory != nil && *gcsFuse.Resources.Memory != "" {
			extraAnnotations["gke-gcsfuse/memory-limit"] = *gcsFuse.Resources.Memory
			extraAnnotations["gke-gcsfuse/memory-request"] = *gcsFuse.Resources.Memory
		}

		if gcsFuse.Resources.EphemeralStorage != nil && *gcsFuse.Resources.EphemeralStorage != "" {
			extraAnnotations["gke-gcsfuse/ephemeral-storage-limit"] = *gcsFuse.Resources.EphemeralStorage
			extraAnnotations["gke-gcsfuse/ephemeral-storage-request"] = *gcsFuse.Resources.EphemeralStorage
		}
	}

	if rayClusterSpec.HeadGroupSpec.Template.ObjectMetaApplyConfiguration == nil || rayClusterSpec.HeadGroupSpec.Template.ObjectMetaApplyConfiguration.Annotations == nil {
		rayClusterSpec.HeadGroupSpec.Template.ObjectMetaApplyConfiguration = &metav1.ObjectMetaApplyConfiguration{
			Annotations: extraAnnotations,
		}
	} else {
		maps.Copy(rayClusterSpec.HeadGroupSpec.Template.Annotations, extraAnnotations)
	}

	for _, workerGroup := range rayClusterSpec.WorkerGroupSpecs {
		if workerGroup.Template.ObjectMetaApplyConfiguration == nil || workerGroup.Template.ObjectMetaApplyConfiguration.Annotations == nil {
			workerGroup.Template.ObjectMetaApplyConfiguration = &metav1.ObjectMetaApplyConfiguration{
				Annotations: extraAnnotations,
			}
		} else {
			maps.Copy(workerGroup.Template.Annotations, extraAnnotations)
		}
	}
}

func ConvertRayClusterSpecApplyConfigToYaml(rayClusterSpec *rayv1ac.RayClusterSpecApplyConfiguration) (string, error) {
	resource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rayClusterSpec)
	if err != nil {
		return "", err
	}

	podByte, err := yaml.Marshal(resource)
	if err != nil {
		return "", err
	}

	return string(podByte), nil
}

// Converts RayClusterApplyConfiguration object into a yaml string
func ConvertRayClusterApplyConfigToYaml(rayClusterac *rayv1ac.RayClusterApplyConfiguration) (string, error) {
	resource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rayClusterac)
	if err != nil {
		return "", err
	}

	podByte, err := yaml.Marshal(resource)
	if err != nil {
		return "", err
	}

	return string(podByte), nil
}

// Converts RayJobApplyConfiguration object into a yaml string
func ConvertRayJobApplyConfigToYaml(rayJobac *rayv1ac.RayJobApplyConfiguration) (string, error) {
	resource, err := runtime.DefaultUnstructuredConverter.ToUnstructured(rayJobac)
	if err != nil {
		return "", err
	}

	podByte, err := yaml.Marshal(resource)
	if err != nil {
		return "", err
	}

	return string(podByte), nil
}

// newRayClusterConfigWithDefaults returns a new RayClusterConfig object with default values
func newRayClusterConfigWithDefaults() *RayClusterConfig {
	return &RayClusterConfig{
		RayVersion: ptr.To(util.RayVersion),
		Image:      new(fmt.Sprintf("rayproject/ray:%s", util.RayVersion)),
		Head: &Head{
			CPU:    ptr.To(util.DefaultHeadCPU),
			Memory: ptr.To(util.DefaultHeadMemory),
		},
		WorkerGroups: []WorkerGroup{
			{
				Name:     new("default-group"),
				Replicas: util.DefaultWorkerReplicas,
				CPU:      ptr.To(util.DefaultWorkerCPU),
				Memory:   ptr.To(util.DefaultWorkerMemory),
			},
		},
	}
}

// ParseConfigFile parses the YAML configuration file into a RayClusterConfig object
func ParseConfigFile(filePath string) (*RayClusterConfig, error) {
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file %s does not exist", filePath)
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	config := newRayClusterConfigWithDefaults()
	if err := yaml.UnmarshalStrict(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse YAML: %w", err)
	}

	return config, nil
}

// ValidateConfig validates the RayClusterConfig object
func ValidateConfig(config *RayClusterConfig) error {
	// Validate head resource quantities
	resourceFields := map[string]*string{
		"cpu":               config.Head.CPU,
		"gpu":               config.Head.GPU,
		"memory":            config.Head.Memory,
		"ephemeral-storage": config.Head.EphemeralStorage,
	}

	for name, value := range resourceFields {
		if value == nil {
			continue
		}
		if err := util.ValidateResourceQuantity(*value, name); err != nil {
			return fmt.Errorf("%w", err)
		}
	}

	// Validate worker groups
	for i, workerGroup := range config.WorkerGroups {
		workerResourceFields := map[string]*string{
			"cpu":               workerGroup.CPU,
			"gpu":               workerGroup.GPU,
			"tpu":               workerGroup.TPU,
			"memory":            workerGroup.Memory,
			"ephemeral-storage": workerGroup.EphemeralStorage,
		}

		for name, value := range workerResourceFields {
			if value == nil {
				continue
			}
			if err := util.ValidateResourceQuantity(*value, fmt.Sprintf("%s (worker group %d)", name, i)); err != nil {
				return fmt.Errorf("%w", err)
			}
		}

		if err := util.ValidateTPU(workerGroup.TPU, workerGroup.NumOfHosts, workerGroup.NodeSelectors); err != nil {
			return fmt.Errorf("%w", err)
		}
	}

	if config.GKE != nil && config.GKE.GCSFuse != nil {
		if err := validateGCSFuse(config.GKE.GCSFuse); err != nil {
			return fmt.Errorf("%w", err)
		}
	}

	return nil
}

func validateGCSFuse(gcsfuse *GCSFuse) error {
	if gcsfuse == nil {
		return nil
	}

	if gcsfuse.BucketName == "" {
		return fmt.Errorf(".gcsfuse.bucket-name is required")
	}

	if gcsfuse.MountPath == "" {
		return fmt.Errorf(".gcsfuse.mount-path is required")
	}

	if gcsfuse.Resources != nil {
		resourceFields := map[string]*string{
			"cpu":               gcsfuse.Resources.CPU,
			"memory":            gcsfuse.Resources.Memory,
			"ephemeral-storage": gcsfuse.Resources.EphemeralStorage,
		}

		for name, value := range resourceFields {
			if value == nil {
				continue
			}
			if err := util.ValidateResourceQuantity(*value, fmt.Sprintf("gcsfuse.resources.%s", name)); err != nil {
				return fmt.Errorf("%w", err)
			}
		}
	}

	return nil
}

// getGCSFuseVolumeAttributes returns a map of volume attributes for the Cloud Storage FUSE CSI driver
// See https://cloud.google.com/kubernetes-engine/docs/reference/cloud-storage-fuse-csi-driver/volume-attr
func getGCSFuseVolumeAttributes(gcsfuse *GCSFuse) map[string]string {
	volumeAttributes := map[string]string{
		"bucketName": gcsfuse.BucketName,
	}

	if gcsfuse.MountOptions != nil && *gcsfuse.MountOptions != "" {
		volumeAttributes["mountOptions"] = *gcsfuse.MountOptions
	}

	if gcsfuse.DisableMetrics != nil {
		volumeAttributes["disableMetrics"] = strconv.FormatBool(*gcsfuse.DisableMetrics)
	}

	if gcsfuse.GCSFuseMetadataPrefetchOnMount != nil {
		volumeAttributes["gcsfuseMetadataPrefetchOnMount"] = strconv.FormatBool(*gcsfuse.GCSFuseMetadataPrefetchOnMount)
	}

	if gcsfuse.SkipCSIBucketAccessCheck != nil {
		volumeAttributes["skipCSIBucketAccessCheck"] = strconv.FormatBool(*gcsfuse.SkipCSIBucketAccessCheck)
	}

	return volumeAttributes
}
