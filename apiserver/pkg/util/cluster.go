package util

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

type RayCluster struct {
	*rayv1api.RayCluster
}

// NewRayCluster creates a RayCluster.
// func NewRayCluster(apiCluster *api.Cluster, clusterRuntime *api.ClusterRuntime, computeRuntime *api.ComputeRuntime) *RayCluster {
func NewRayCluster(apiCluster *api.Cluster, computeTemplateMap map[string]*api.ComputeTemplate) (*RayCluster, error) {
	// Check for "ray.io/enable-serve-service=true"
	enableServeService := false
	if enableServeServiceValue, exist := apiCluster.Annotations["ray.io/enable-serve-service"]; exist && enableServeServiceValue == "true" {
		enableServeService = true
	}

	// Build cluster spec
	spec, err := buildRayClusterSpec(apiCluster.Version, apiCluster.Envs, apiCluster.ClusterSpec, computeTemplateMap, enableServeService)
	if err != nil {
		return nil, err
	}
	// Build cluster
	rayCluster := &rayv1api.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:        apiCluster.Name,
			Namespace:   apiCluster.Namespace,
			Labels:      buildRayClusterLabels(apiCluster),
			Annotations: buildRayClusterAnnotations(apiCluster),
		},
		Spec: *spec,
	}

	return &RayCluster{rayCluster}, nil
}

// Build cluster labels
func buildRayClusterLabels(cluster *api.Cluster) map[string]string {
	labels := map[string]string{}
	labels[RayClusterNameLabelKey] = cluster.Name
	labels[RayClusterUserLabelKey] = cluster.User
	labels[RayClusterVersionLabelKey] = cluster.Version
	labels[RayClusterEnvironmentLabelKey] = cluster.Environment.String()
	labels[KubernetesApplicationNameLabelKey] = ApplicationName
	labels[KubernetesManagedByLabelKey] = ComponentName
	return labels
}

// Build cluster annotations
func buildRayClusterAnnotations(cluster *api.Cluster) map[string]string {
	if cluster.Annotations == nil {
		return map[string]string{}
	}
	return cluster.Annotations
}

// TODO(Basasuya & MissionToMars): The job spec depends on ClusterSpec which not all cluster-related configs are included,
// such as `metadata` and `envs`. We just put `imageVersion` and `envs` in the arguments list, and should be refactored later.
func buildRayClusterSpec(imageVersion string, envs *api.EnvironmentVariables, clusterSpec *api.ClusterSpec, computeTemplateMap map[string]*api.ComputeTemplate, enableServeService bool) (*rayv1api.RayClusterSpec, error) {
	computeTemplate := computeTemplateMap[clusterSpec.HeadGroupSpec.ComputeTemplate]
	headPodTemplate, err := buildHeadPodTemplate(imageVersion, envs, clusterSpec.HeadGroupSpec, computeTemplate, enableServeService)
	if err != nil {
		return nil, err
	}
	rayClusterSpec := &rayv1api.RayClusterSpec{
		RayVersion: imageVersion,
		HeadGroupSpec: rayv1api.HeadGroupSpec{
			ServiceType:    corev1.ServiceType(clusterSpec.HeadGroupSpec.ServiceType),
			Template:       *headPodTemplate,
			RayStartParams: clusterSpec.HeadGroupSpec.RayStartParams,
		},
		WorkerGroupSpecs:       []rayv1api.WorkerGroupSpec{},
		HeadServiceAnnotations: clusterSpec.HeadServiceAnnotations,
	}

	// If enable ingress is specified, add it to the head node spec.
	if clusterSpec.HeadGroupSpec.EnableIngress {
		rayClusterSpec.HeadGroupSpec.EnableIngress = &clusterSpec.HeadGroupSpec.EnableIngress
	}

	// Build worker groups
	for _, spec := range clusterSpec.WorkerGroupSpec {
		computeTemplate = computeTemplateMap[spec.ComputeTemplate]
		workerPodTemplate, err := buildWorkerPodTemplate(imageVersion, envs, spec, computeTemplate)
		if err != nil {
			return nil, err
		}

		minReplicas := spec.Replicas
		maxReplicas := spec.Replicas
		if spec.MinReplicas != 0 {
			minReplicas = spec.MinReplicas
		}
		if spec.MaxReplicas != 0 {
			maxReplicas = spec.MaxReplicas
		}

		workerNodeSpec := rayv1api.WorkerGroupSpec{
			GroupName:      spec.GroupName,
			MinReplicas:    new(minReplicas),
			MaxReplicas:    new(maxReplicas),
			Replicas:       new(spec.Replicas),
			RayStartParams: spec.RayStartParams,
			Template:       *workerPodTemplate,
		}

		rayClusterSpec.WorkerGroupSpecs = append(rayClusterSpec.WorkerGroupSpecs, workerNodeSpec)
	}

	if clusterSpec.EnableInTreeAutoscaling {
		// This is a cluster with auto scaler
		rayClusterSpec.EnableInTreeAutoscaling = &clusterSpec.EnableInTreeAutoscaling
		options, err := buildAutoscalerOptions(clusterSpec.AutoscalerOptions)
		if err != nil {
			return nil, err
		}
		if options != nil {
			rayClusterSpec.AutoscalerOptions = options
		}
	}

	return rayClusterSpec, nil
}

// Annotations common to both head and worker nodes
func buildNodeGroupAnnotations(computeTemplate *api.ComputeTemplate, image string) map[string]string {
	annotations := map[string]string{}
	annotations[RayClusterComputeTemplateAnnotationKey] = computeTemplate.Name
	annotations[RayClusterImageAnnotationKey] = image
	return annotations
}

// Add resource to container
func addResourceToContainer(container *corev1.Container, resourceName string, quantity uint32) {
	if quantity == 0 {
		return
	}
	quantityStr := fmt.Sprint(quantity)
	resourceQuantity := resource.MustParse(quantityStr)

	if container.Resources.Requests == nil {
		container.Resources.Requests = make(corev1.ResourceList)
	}
	if container.Resources.Limits == nil {
		container.Resources.Limits = make(corev1.ResourceList)
	}

	container.Resources.Requests[corev1.ResourceName(resourceName)] = resourceQuantity
	container.Resources.Limits[corev1.ResourceName(resourceName)] = resourceQuantity
}

// Build head node template
func buildHeadPodTemplate(imageVersion string, envs *api.EnvironmentVariables, spec *api.HeadGroupSpec, computeRuntime *api.ComputeTemplate, enableServeService bool) (*corev1.PodTemplateSpec, error) {
	image := constructRayImage(RayClusterDefaultImageRepository, imageVersion)
	if len(spec.Image) != 0 {
		image = spec.Image
	}

	// Image pull policy. Kubernetes default image pull policy IfNotPresent, so we here only
	// Overwrite it if it is Always
	imagePullPolicy := corev1.PullIfNotPresent
	if len(spec.ImagePullPolicy) > 0 && strings.ToLower(spec.ImagePullPolicy) == "always" {
		imagePullPolicy = corev1.PullAlways
	}

	// calculate resources
	cpu := fmt.Sprint(computeRuntime.GetCpu())
	memoryUnit := computeRuntime.GetMemoryUnit()
	memory := fmt.Sprintf("%d%s", computeRuntime.GetMemory(), memoryUnit)

	// build volume and volumeMounts
	volMounts := buildVolumeMounts(spec.Volumes)
	vols, err := buildVols(spec.Volumes)
	if err != nil {
		return nil, err
	}

	podTemplateSpec := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: buildNodeGroupAnnotations(computeRuntime, spec.Image),
			Labels:      map[string]string{},
		},
		Spec: corev1.PodSpec{
			Tolerations: []corev1.Toleration{},
			Containers: []corev1.Container{
				{
					Name:            "ray-head",
					Image:           image,
					ImagePullPolicy: imagePullPolicy,
					Env: []corev1.EnvVar{
						{
							Name: "MY_POD_IP",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "status.podIP",
								},
							},
						},
					},
					// Customization is not allowed here. We should consider whether to make this part smart.
					// For now we use serve 8000 port for rayservice and added at util/service.go, do not use the 8000 port here for other purpose.
					Ports: []corev1.ContainerPort{
						{
							Name:          "redis",
							ContainerPort: 6379,
						},
						{
							Name:          "head",
							ContainerPort: 10001,
						},
						{
							Name:          "dashboard",
							ContainerPort: 8265,
						},
						{
							Name:          "metrics",
							ContainerPort: 8080,
						},
					},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(cpu),
							corev1.ResourceMemory: resource.MustParse(memory),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(cpu),
							corev1.ResourceMemory: resource.MustParse(memory),
						},
					},
					VolumeMounts:    volMounts,
					SecurityContext: buildSecurityContext(spec.SecurityContext),
				},
			},
			Volumes: vols,
		},
	}

	// We are filtering container by name `ray-head`. If container with this name does not exist
	// (should never happen) we are not adding container specific parameters
	if container, index, ok := GetContainerByName(podTemplateSpec.Spec.Containers, "ray-head"); ok {
		if gpu := computeRuntime.GetGpu(); gpu != 0 {
			accelerator := "nvidia.com/gpu"
			if len(computeRuntime.GetGpuAccelerator()) != 0 {
				accelerator = computeRuntime.GetGpuAccelerator()
			}
			addResourceToContainer(&container, accelerator, gpu)
		}

		for k, v := range computeRuntime.GetExtendedResources() {
			addResourceToContainer(&container, k, v)
		}

		globalEnv := convertEnvironmentVariables(envs)
		if len(globalEnv) > 0 {
			container.Env = append(container.Env, globalEnv...)
		}

		// Add specific environments
		specEnv := convertEnvironmentVariables(spec.Environment)
		if len(specEnv) > 0 {
			container.Env = append(container.Env, specEnv...)
		}

		// If enableServeService add port
		if enableServeService {
			container.Ports = append(container.Ports, corev1.ContainerPort{Name: "dashboard-agent", ContainerPort: 52365})
			container.Ports = append(container.Ports, corev1.ContainerPort{Name: "serve", ContainerPort: 8000})
		}

		// Replace container
		podTemplateSpec.Spec.Containers[index] = container
	}

	// Add specific annotations
	if spec.Annotations != nil {
		maps.Copy(podTemplateSpec.ObjectMeta.Annotations, spec.Annotations)
	}

	// Add specific labels
	if spec.Labels != nil {
		maps.Copy(podTemplateSpec.ObjectMeta.Labels, spec.Labels)
	}

	// Add specific tollerations
	if computeRuntime.Tolerations != nil {
		for _, t := range computeRuntime.Tolerations {
			podTemplateSpec.Spec.Tolerations = append(podTemplateSpec.Spec.Tolerations, corev1.Toleration{
				Key: t.Key, Operator: convertTolerationOperator(t.Operator), Value: t.Value, Effect: convertTaintEffect(t.Effect),
			})
		}
	}

	// If service account is specified, add it to the pod spec.
	if len(spec.ServiceAccount) > 1 {
		podTemplateSpec.Spec.ServiceAccountName = spec.ServiceAccount
	}

	// If image pull secret is specified, add it to the pod spec.
	if len(spec.ImagePullSecret) > 1 {
		podTemplateSpec.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{
				Name: spec.ImagePullSecret,
			},
		}
	}

	return &podTemplateSpec, nil
}

// Convert environment variables
func convertEnvironmentVariables(envs *api.EnvironmentVariables) []corev1.EnvVar {
	converted := []corev1.EnvVar{}
	if envs == nil {
		return converted
	}
	if len(envs.Values) > 0 {
		// Add values
		for key, value := range envs.Values {
			converted = append(converted, corev1.EnvVar{
				Name: key, Value: value,
			})
		}
	}
	if len(envs.ValuesFrom) > 0 {
		// Add values ref
		for key, value := range envs.ValuesFrom {
			switch value.Source {
			case api.EnvValueFrom_CONFIGMAP:
				converted = append(converted, corev1.EnvVar{
					Name: key,
					ValueFrom: &corev1.EnvVarSource{
						ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: value.Name,
							},
							Key: value.Key,
						},
					},
				})
			case api.EnvValueFrom_SECRET:
				converted = append(converted, corev1.EnvVar{
					Name: key,
					ValueFrom: &corev1.EnvVarSource{
						SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{
								Name: value.Name,
							},
							Key: value.Key,
						},
					},
				})
			case api.EnvValueFrom_RESOURCEFIELD:
				converted = append(converted, corev1.EnvVar{
					Name: key,
					ValueFrom: &corev1.EnvVarSource{
						ResourceFieldRef: &corev1.ResourceFieldSelector{
							ContainerName: value.Name,
							Resource:      value.Key,
						},
					},
				})
			default:
				converted = append(converted, corev1.EnvVar{
					Name: key,
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: value.Key,
						},
					},
				})
			}
		}
	}
	return converted
}

// Convert Toleration operator from string
func convertTolerationOperator(val string) corev1.TolerationOperator {
	if val == "Exists" {
		return corev1.TolerationOpExists
	}
	return corev1.TolerationOpEqual
}

// Convert taint effect from string
func convertTaintEffect(val string) corev1.TaintEffect {
	if val == "NoExecute" {
		return corev1.TaintEffectNoExecute
	}
	if val == "NoSchedule" {
		return corev1.TaintEffectNoSchedule
	}
	return corev1.TaintEffectPreferNoSchedule
}

// Construct Ray image
func constructRayImage(containerImage string, version string) string {
	return fmt.Sprintf("%s:%s", containerImage, version)
}

// Build worker pod template
func buildWorkerPodTemplate(imageVersion string, envs *api.EnvironmentVariables, spec *api.WorkerGroupSpec, computeRuntime *api.ComputeTemplate) (*corev1.PodTemplateSpec, error) {
	// If user doesn't provide the image, let's use the default image instead.
	// TODO: verify the versions in the range
	image := constructRayImage(RayClusterDefaultImageRepository, imageVersion)
	if len(spec.Image) != 0 {
		image = spec.Image
	}

	// Image pull policy. Kubernetes default image pull policy IfNotPresent, so we here only
	// Overwrite it if it is Always
	imagePullPolicy := corev1.PullIfNotPresent
	if len(spec.ImagePullPolicy) > 0 && strings.ToLower(spec.ImagePullPolicy) == "always" {
		imagePullPolicy = corev1.PullAlways
	}

	// calculate resources
	cpu := fmt.Sprint(computeRuntime.GetCpu())
	memoryUnit := computeRuntime.GetMemoryUnit()
	memory := fmt.Sprintf("%d%s", computeRuntime.GetMemory(), memoryUnit)

	// build volume and volumeMounts
	volMounts := buildVolumeMounts(spec.Volumes)
	vols, err := buildVols(spec.Volumes)
	if err != nil {
		return nil, err
	}

	podTemplateSpec := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: buildNodeGroupAnnotations(computeRuntime, spec.Image),
			Labels:      map[string]string{},
		},
		Spec: corev1.PodSpec{
			Tolerations: []corev1.Toleration{},
			Containers: []corev1.Container{
				{
					Name:            "ray-worker",
					Image:           image,
					ImagePullPolicy: imagePullPolicy,
					Env: []corev1.EnvVar{
						{
							Name:  "RAY_DISABLE_DOCKER_CPU_WARNING",
							Value: "1",
						},
						{
							Name:  "TYPE",
							Value: "worker",
						},
						{
							Name: "CPU_REQUEST",
							ValueFrom: &corev1.EnvVarSource{
								ResourceFieldRef: &corev1.ResourceFieldSelector{
									ContainerName: "ray-worker",
									Resource:      "requests.cpu",
								},
							},
						},
						{
							Name: "CPU_LIMITS",
							ValueFrom: &corev1.EnvVarSource{
								ResourceFieldRef: &corev1.ResourceFieldSelector{
									ContainerName: "ray-worker",
									Resource:      "limits.cpu",
								},
							},
						},
						{
							Name: "MEMORY_REQUESTS",
							ValueFrom: &corev1.EnvVarSource{
								ResourceFieldRef: &corev1.ResourceFieldSelector{
									ContainerName: "ray-worker",
									Resource:      "requests.memory",
								},
							},
						},
						{
							Name: "MEMORY_LIMITS",
							ValueFrom: &corev1.EnvVarSource{
								ResourceFieldRef: &corev1.ResourceFieldSelector{
									ContainerName: "ray-worker",
									Resource:      "limits.memory",
								},
							},
						},
						{
							Name: "MY_POD_NAME",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "metadata.name",
								},
							},
						},
						{
							Name: "MY_POD_IP",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "status.podIP",
								},
							},
						},
					},
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: 80,
						},
					},
					Lifecycle: &corev1.Lifecycle{
						PreStop: &corev1.LifecycleHandler{
							Exec: &corev1.ExecAction{
								Command: []string{
									"/bin/sh", "-c", "ray stop",
								},
							},
						},
					},
					Resources: corev1.ResourceRequirements{
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(cpu),
							corev1.ResourceMemory: resource.MustParse(memory),
						},
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    resource.MustParse(cpu),
							corev1.ResourceMemory: resource.MustParse(memory),
						},
					},
					VolumeMounts:    volMounts,
					SecurityContext: buildSecurityContext(spec.SecurityContext),
				},
			},
			Volumes: vols,
		},
	}

	// We are filtering container by name `ray-worker`. If container with this name does not exist
	// (should never happen) we are not adding container specific parameters
	if container, index, ok := GetContainerByName(podTemplateSpec.Spec.Containers, "ray-worker"); ok {
		if gpu := computeRuntime.GetGpu(); gpu != 0 {
			accelerator := "nvidia.com/gpu"
			if len(computeRuntime.GetGpuAccelerator()) != 0 {
				accelerator = computeRuntime.GetGpuAccelerator()
			}
			addResourceToContainer(&container, accelerator, gpu)
		}

		for k, v := range computeRuntime.GetExtendedResources() {
			addResourceToContainer(&container, k, v)
		}

		globalEnv := convertEnvironmentVariables(envs)
		if len(globalEnv) > 0 {
			container.Env = append(container.Env, globalEnv...)
		}

		// Add specific environments
		specEnv := convertEnvironmentVariables(spec.Environment)
		if len(specEnv) > 0 {
			container.Env = append(container.Env, specEnv...)
		}

		// Replace container
		podTemplateSpec.Spec.Containers[index] = container
	}

	// Add specific annotations
	if spec.Annotations != nil {
		maps.Copy(podTemplateSpec.ObjectMeta.Annotations, spec.Annotations)
	}

	// Add specific labels
	if spec.Labels != nil {
		maps.Copy(podTemplateSpec.ObjectMeta.Labels, spec.Labels)
	}

	// Add specific tollerations
	if computeRuntime.Tolerations != nil {
		for _, t := range computeRuntime.Tolerations {
			podTemplateSpec.Spec.Tolerations = append(podTemplateSpec.Spec.Tolerations, corev1.Toleration{
				Key: t.Key, Operator: convertTolerationOperator(t.Operator), Value: t.Value, Effect: convertTaintEffect(t.Effect),
			})
		}
	}

	// If service account is specified, add it to the pod spec.
	if len(spec.ServiceAccount) > 1 {
		podTemplateSpec.Spec.ServiceAccountName = spec.ServiceAccount
	}

	// If image pull secret is specified, add it to the pod spec.
	if len(spec.ImagePullSecret) > 1 {
		podTemplateSpec.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{
				Name: spec.ImagePullSecret,
			},
		}
	}

	return &podTemplateSpec, nil
}

// Build Volume mounts
func buildVolumeMounts(apiVolumes []*api.Volume) []corev1.VolumeMount {
	var (
		volMounts       []corev1.VolumeMount
		hostToContainer = corev1.MountPropagationHostToContainer
		bidirectonal    = corev1.MountPropagationBidirectional
	)
	for _, vol := range apiVolumes {
		volMount := corev1.VolumeMount{
			Name:      vol.Name,
			ReadOnly:  vol.ReadOnly,
			MountPath: vol.MountPath,
		}
		switch vol.MountPropagationMode {
		case api.Volume_HOSTTOCONTAINER:
			volMount.MountPropagation = &hostToContainer
		case api.Volume_BIDIRECTIONAL:
			volMount.MountPropagation = &bidirectonal
		}
		volMounts = append(volMounts, volMount)
	}
	return volMounts
}

// Build host path
func newHostPathType(pathType string) *corev1.HostPathType {
	hostPathType := new(corev1.HostPathType)
	*hostPathType = corev1.HostPathType(pathType)
	return hostPathType
}

// Build volumes
func buildVols(apiVolumes []*api.Volume) ([]corev1.Volume, error) {
	var vols []corev1.Volume
	for _, rayVol := range apiVolumes {
		if rayVol.VolumeType == api.Volume_CONFIGMAP {
			vol := corev1.Volume{
				Name: rayVol.Name,
				VolumeSource: corev1.VolumeSource{
					ConfigMap: &corev1.ConfigMapVolumeSource{
						LocalObjectReference: corev1.LocalObjectReference{
							Name: rayVol.Source,
						},
					},
				},
			}
			if len(rayVol.Items) > 0 {
				// Add items
				items := []corev1.KeyToPath{}
				for key, value := range rayVol.Items {
					items = append(items, corev1.KeyToPath{Key: key, Path: value})
				}
				vol.ConfigMap.Items = items
			}
			vols = append(vols, vol)
		}
		if rayVol.VolumeType == api.Volume_SECRET {
			vol := corev1.Volume{
				Name: rayVol.Name,
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: rayVol.Source,
					},
				},
			}
			if len(rayVol.Items) > 0 {
				// Add items
				items := []corev1.KeyToPath{}
				for key, value := range rayVol.Items {
					items = append(items, corev1.KeyToPath{Key: key, Path: value})
				}
				vol.Secret.Items = items
			}
			vols = append(vols, vol)
		}
		if rayVol.VolumeType == api.Volume_EMPTY_DIR {
			vol := corev1.Volume{
				Name: rayVol.Name,
				VolumeSource: corev1.VolumeSource{
					EmptyDir: &corev1.EmptyDirVolumeSource{},
				},
			}
			if rayVol.Storage != "" {
				// Max Storage size is  defined
				// Ensure that storage size is formatted correctly
				_, err := resource.ParseQuantity(rayVol.Storage)
				if err != nil {
					return nil, errors.New("storage for empty dir volume is not specified correctly")
				}
				limit := resource.MustParse(rayVol.Storage)
				vol.EmptyDir.SizeLimit = &limit
			}
			vols = append(vols, vol)
		}
		if rayVol.VolumeType == api.Volume_HOST_PATH {
			vol := corev1.Volume{
				Name: rayVol.Name,
				VolumeSource: corev1.VolumeSource{
					HostPath: &corev1.HostPathVolumeSource{
						Path: rayVol.Source,
					},
				},
			}
			switch rayVol.HostPathType {
			case api.Volume_DIRECTORY:
				vol.VolumeSource.HostPath.Type = newHostPathType(string(corev1.HostPathDirectory))
			case api.Volume_FILE:
				vol.VolumeSource.HostPath.Type = newHostPathType(string(corev1.HostPathFile))
			default:
				vol.VolumeSource.HostPath.Type = newHostPathType(string(corev1.HostPathDirectory))
			}
			vols = append(vols, vol)
		}
		if rayVol.VolumeType == api.Volume_PERSISTENT_VOLUME_CLAIM {
			vol := corev1.Volume{
				Name: rayVol.Name,
				VolumeSource: corev1.VolumeSource{
					PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
						ClaimName: rayVol.Source,
						ReadOnly:  rayVol.ReadOnly,
					},
				},
			}
			vols = append(vols, vol)
		}
		if rayVol.VolumeType == api.Volume_EPHEMERAL {
			// Make sure that at least the storage size is defined
			if rayVol.Storage == "" {
				// Storage size is not defined
				return nil, errors.New("storage for ephemeral volume is empty")
			}
			// Ensure that storage size is formatted correctly
			_, err := resource.ParseQuantity(rayVol.Storage)
			if err != nil {
				return nil, errors.New("storage for ephemeral volume is not specified correctly")
			}
			vol := corev1.Volume{
				Name: rayVol.Name,
				VolumeSource: corev1.VolumeSource{
					Ephemeral: &corev1.EphemeralVolumeSource{
						VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
							ObjectMeta: metav1.ObjectMeta{
								Labels: map[string]string{
									"app.kubernetes.io/managed-by": "kuberay-apiserver",
								},
							},
							Spec: corev1.PersistentVolumeClaimSpec{
								Resources: corev1.VolumeResourceRequirements{
									Requests: corev1.ResourceList{
										corev1.ResourceStorage: resource.MustParse(rayVol.Storage),
									},
								},
							},
						},
					},
				},
			}
			if len(rayVol.StorageClassName) > 0 {
				// Populate storage class, if defined
				vol.VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.StorageClassName = &rayVol.StorageClassName
			}

			// Populate access mode if defined
			switch rayVol.AccessMode {
			case api.Volume_RWO:
				vol.VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				}
			case api.Volume_RWX:
				vol.VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteMany,
				}
			case api.Volume_ROX:
				vol.VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{
					corev1.ReadOnlyMany,
				}
			default:
				vol.VolumeSource.Ephemeral.VolumeClaimTemplate.Spec.AccessModes = []corev1.PersistentVolumeAccessMode{
					corev1.ReadWriteOnce,
				}
			}
			vols = append(vols, vol)
		}
	}

	return vols, nil
}

// Build security context
func buildSecurityContext(securityCtx *api.SecurityContext) *corev1.SecurityContext {
	if securityCtx == nil {
		return nil
	}
	result := &corev1.SecurityContext{
		Privileged:   securityCtx.Privileged,
		Capabilities: &corev1.Capabilities{},
	}
	if securityCtx.Capabilities != nil {
		for _, cap := range securityCtx.Capabilities.Add {
			result.Capabilities.Add = append(result.Capabilities.Add, corev1.Capability(cap))
		}
		for _, cap := range securityCtx.Capabilities.Drop {
			result.Capabilities.Drop = append(result.Capabilities.Drop, corev1.Capability(cap))
		}
	}
	return result
}

// Get converts this object to a rayv1api.Workflow.
func (c *RayCluster) Get() *rayv1api.RayCluster {
	return c.RayCluster
}

// SetAnnotations sets annotations on all templates in a RayCluster
func (c *RayCluster) SetAnnotationsToAllTemplates(_ string, _ string) {
	// TODO: reserved for common parameters.
}

// Build compute template
func NewComputeTemplate(runtime *api.ComputeTemplate) (*corev1.ConfigMap, error) {
	extendedResourcesJSON, err := json.Marshal(runtime.ExtendedResources)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal extended resources: %w", err)
	}
	memoryUnit := "Gi"
	if runtime.MemoryUnit != "" {
		memoryUnit = runtime.MemoryUnit
	}

	memory := strconv.FormatUint(uint64(runtime.Memory), 10)
	quantity := memory + memoryUnit

	if _, err := resource.ParseQuantity(quantity); err != nil {
		return nil, fmt.Errorf("invalid memory quantity %q: %w", quantity, err)
	}

	// Create data map
	dmap := map[string]string{
		"name":               runtime.Name,
		"namespace":          runtime.Namespace,
		"cpu":                strconv.FormatUint(uint64(runtime.Cpu), 10),
		"memory":             memory,
		"memory_unit":        memoryUnit,
		"gpu":                strconv.FormatUint(uint64(runtime.Gpu), 10),
		"gpu_accelerator":    runtime.GpuAccelerator,
		"extended_resources": string(extendedResourcesJSON),
	}
	// Add tolerations in defined
	if len(runtime.Tolerations) > 0 {
		t, err := json.Marshal(runtime.Tolerations)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal tolerations for compute template %s: %w", runtime.Name, err)
		}
		dmap["tolerations"] = string(t)
	}

	config := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      runtime.Name,
			Namespace: runtime.Namespace,
			Labels: map[string]string{
				"ray.io/config-type":      "compute-template",
				"ray.io/compute-template": runtime.Name,
			},
		},
		Data: dmap,
	}

	return config, nil
}

// GetNodeHostIP returns the provided node's IP, based on the priority:
// 1. NodeInternalIP
// 2. NodeExternalIP
func GetNodeHostIP(node *corev1.Node) (net.IP, error) {
	addresses := node.Status.Addresses
	addressMap := make(map[corev1.NodeAddressType][]corev1.NodeAddress)
	for _, nodeAddress := range addresses {
		addressMap[nodeAddress.Type] = append(addressMap[nodeAddress.Type], nodeAddress)
	}
	if addresses, ok := addressMap[corev1.NodeInternalIP]; ok {
		return net.ParseIP(addresses[0].Address), nil
	}
	if addresses, ok := addressMap[corev1.NodeExternalIP]; ok {
		return net.ParseIP(addresses[0].Address), nil
	}
	return nil, fmt.Errorf("host IP unknown; known addresses: %v", addresses)
}

func GetContainerByName(containers []corev1.Container, name string) (corev1.Container, int, bool) {
	for index, container := range containers {
		if container.Name == name {
			return container, index, true
		}
	}
	return corev1.Container{}, 0, false
}

func buildAutoscalerOptions(autoscalerOptions *api.AutoscalerOptions) (*rayv1api.AutoscalerOptions, error) {
	if autoscalerOptions == nil {
		return nil, nil
	}
	options := rayv1api.AutoscalerOptions{}
	if autoscalerOptions.IdleTimeoutSeconds > 0 {
		options.IdleTimeoutSeconds = &autoscalerOptions.IdleTimeoutSeconds
	}
	if len(autoscalerOptions.UpscalingMode) > 0 {
		options.UpscalingMode = (*rayv1api.UpscalingMode)(&autoscalerOptions.UpscalingMode)
	}
	if len(autoscalerOptions.Image) > 0 {
		options.Image = &autoscalerOptions.Image
	}
	if len(autoscalerOptions.ImagePullPolicy) > 0 && strings.ToLower(autoscalerOptions.ImagePullPolicy) == "always" {
		policy := corev1.PullAlways
		options.ImagePullPolicy = &policy
	}

	if autoscalerOptions.Envs != nil {
		if len(autoscalerOptions.Envs.Values) > 0 {
			options.Env = make([]corev1.EnvVar, len(autoscalerOptions.Envs.Values))
			evCount := 0
			for key, value := range autoscalerOptions.Envs.Values {
				options.Env[evCount] = corev1.EnvVar{Name: key, Value: value}
				evCount++
			}
		}
		if len(autoscalerOptions.Envs.ValuesFrom) > 0 {
			options.EnvFrom = make([]corev1.EnvFromSource, 0)
			for _, value := range autoscalerOptions.Envs.ValuesFrom {
				if evfrom := convertEnvFrom(value); evfrom != nil {
					options.EnvFrom = append(options.EnvFrom, *evfrom)
				}
			}
		}
	}
	if len(autoscalerOptions.Volumes) > 0 {
		options.VolumeMounts = buildVolumeMounts(autoscalerOptions.Volumes)
	}
	if len(autoscalerOptions.Cpu) > 0 || len(autoscalerOptions.Memory) > 0 {
		rcpu := "500m"
		rmemory := "512Mi"
		if len(autoscalerOptions.Cpu) > 0 {
			rcpu = autoscalerOptions.Cpu
		}
		if len(autoscalerOptions.Memory) > 0 {
			rmemory = autoscalerOptions.Memory
		}
		_, err := resource.ParseQuantity(rcpu)
		if err != nil {
			return nil, errors.New("cpu for autoscaler is not specified correctly")
		}
		_, err = resource.ParseQuantity(rmemory)
		if err != nil {
			return nil, errors.New("memory for autoscaler is not specified correctly")
		}
		options.Resources = &corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(rcpu),
				corev1.ResourceMemory: resource.MustParse(rmemory),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(rcpu),
				corev1.ResourceMemory: resource.MustParse(rmemory),
			},
		}
	}
	return &options, nil
}

func convertEnvFrom(from *api.EnvValueFrom) *corev1.EnvFromSource {
	switch from.Source {
	case api.EnvValueFrom_CONFIGMAP:
		return &corev1.EnvFromSource{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: from.Name,
				},
			},
		}
	case api.EnvValueFrom_SECRET:
		return &corev1.EnvFromSource{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: from.Name,
				},
			},
		}
	default:
		return nil
	}
}
