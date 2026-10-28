package util

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

type RayJob struct {
	*rayv1api.RayJob
}

// NewRayJob creates a RayJob.
func NewRayJob(apiJob *api.RayJob, computeTemplateMap map[string]*api.ComputeTemplate) (*RayJob, error) {
	rayJob := &rayv1api.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:        apiJob.Name,
			Namespace:   apiJob.Namespace,
			Labels:      buildRayJobLabels(apiJob),
			Annotations: buildRayJobAnnotations(apiJob),
		},
		Spec: rayv1api.RayJobSpec{
			Entrypoint:               apiJob.Entrypoint,
			Metadata:                 apiJob.Metadata,
			RuntimeEnvYAML:           apiJob.RuntimeEnv,
			ShutdownAfterJobFinishes: apiJob.ShutdownAfterJobFinishes,
			TTLSecondsAfterFinished:  apiJob.TtlSecondsAfterFinished,
			JobId:                    apiJob.JobId,
			RayClusterSpec:           nil,
			ClusterSelector:          apiJob.ClusterSelector,
		},
	}
	if apiJob.ClusterSpec != nil {
		clusterSpec, err := buildRayClusterSpec(apiJob.Version, nil, apiJob.ClusterSpec, computeTemplateMap, false)
		if err != nil {
			return nil, err
		}
		rayJob.Spec.RayClusterSpec = clusterSpec
	}
	if apiJob.JobSubmitter != nil {
		// Job submitter is specified, create SubmitterPodTemplate
		cpus := "1"
		memorys := "1Gi"
		if apiJob.JobSubmitter.Cpu != "" {
			// Ensure that CPU size is formatted correctly
			_, err := resource.ParseQuantity(apiJob.JobSubmitter.Cpu)
			if err != nil {
				return nil, fmt.Errorf("cpu for ray job submitter %s is not specified correctly: %w", apiJob.JobSubmitter.Cpu, err)
			}
			cpus = apiJob.JobSubmitter.Cpu
		}
		if apiJob.JobSubmitter.Memory != "" {
			// Ensure that memory size is formatted correctly
			_, err := resource.ParseQuantity(apiJob.JobSubmitter.Memory)
			if err != nil {
				return nil, fmt.Errorf("memory for ray job submitter %s is not specified correctly: %w", apiJob.JobSubmitter.Memory, err)
			}
			memorys = apiJob.JobSubmitter.Memory
		}
		rayJob.Spec.SubmitterPodTemplate = &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "ray-job-submitter",
						Image: apiJob.JobSubmitter.Image,
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse(cpus),
								corev1.ResourceMemory: resource.MustParse(memorys),
							},
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse(cpus),
								corev1.ResourceMemory: resource.MustParse(memorys),
							},
						},
					},
				},
				RestartPolicy: corev1.RestartPolicyNever,
			},
		}
	}
	if apiJob.EntrypointNumCpus > 0 {
		// Entry point number of CPUs
		rayJob.Spec.EntrypointNumCpus = apiJob.EntrypointNumCpus
	}
	if apiJob.EntrypointNumGpus > 0 {
		// Entry point number of GPUs
		rayJob.Spec.DeepCopy().EntrypointNumGpus = apiJob.EntrypointNumGpus
	}
	if apiJob.EntrypointResources != "" {
		// Entry point resources
		rayJob.Spec.EntrypointResources = apiJob.EntrypointResources
	}
	if apiJob.ActiveDeadlineSeconds > 0 {
		rayJob.Spec.ActiveDeadlineSeconds = &apiJob.ActiveDeadlineSeconds
	}

	return &RayJob{rayJob}, nil
}

func (j *RayJob) Get() *rayv1api.RayJob {
	return j.RayJob
}

func buildRayJobLabels(job *api.RayJob) map[string]string {
	labels := map[string]string{}
	labels[RayClusterNameLabelKey] = job.Name
	labels[RayClusterUserLabelKey] = job.User
	labels[KubernetesApplicationNameLabelKey] = ApplicationName
	labels[KubernetesManagedByLabelKey] = ComponentName
	return labels
}

func buildRayJobAnnotations(job *api.RayJob) map[string]string {
	return job.Metadata
}
