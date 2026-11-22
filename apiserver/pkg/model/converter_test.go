package model

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	util "github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

var (
	enableIngress                  = true
	workerReplicas           int32 = 5
	workerMinReplicas        int32 = 1
	workerMaxReplicas        int32 = 3
	unhealthySecondThreshold int32 = 900
	secondsValue             int32 = 100
)

var headSpecTest = rayv1api.HeadGroupSpec{
	ServiceType:   "ClusterIP",
	EnableIngress: &enableIngress,
	RayStartParams: map[string]string{
		"dashboard-host":      "0.0.0.0",
		"metrics-export-port": "8080",
		"num-cpus":            "0",
	},
	Template: corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"openshift.io/scc":    "restricted",
				"ray.io/ft-enabled":   "false",
				"ray.io/health-state": "",
				"custom":              "value",
			},
			Labels: map[string]string{
				"app.kubernetes.io/created-by": "kuberay-operator",
				"app.kubernetes.io/name":       "kuberay",
				"ray.io/cluster":               "kuberay-cluster",
				"ray.io/cluster-dashboard":     "kuberay-cluster-dashboard",
				"ray.io/group":                 utils.RayNodeHeadGroupLabelValue,
				"ray.io/identifier":            "kuberay-cluster-head",
				"ray.io/is-ray-node":           "yes",
				"ray.io/node-type":             "head",
				"test":                         "value",
			},
			Name:      "kuberay-cluster-head-f7zx2",
			Namespace: "max",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: "account",
			ImagePullSecrets: []corev1.LocalObjectReference{
				{Name: "foo"},
			},
			Tolerations: []corev1.Toleration{
				{
					Key:      "blah1",
					Operator: "Exists",
					Effect:   "NoExecute",
				},
			},
			Containers: []corev1.Container{
				{
					Name:            "ray-head",
					Image:           "blublinsky1/ray310:2.5.0",
					ImagePullPolicy: "Always",
					Env: []corev1.EnvVar{
						{
							Name:  "AWS_KEY",
							Value: "123",
						},
						{
							Name: "REDIS_PASSWORD",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "redis-password-secret",
									},
									Key: "password",
								},
							},
						},
						{
							Name: "CONFIGMAP",
							ValueFrom: &corev1.EnvVarSource{
								ConfigMapKeyRef: &corev1.ConfigMapKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: "special-config",
									},
									Key: "special.how",
								},
							},
						},
						{
							Name: "ResourceFieldRef",
							ValueFrom: &corev1.EnvVarSource{
								ResourceFieldRef: &corev1.ResourceFieldSelector{
									ContainerName: "my-container",
									Resource:      "resource",
								},
							},
						},
						{
							Name: "FieldRef",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "path",
								},
							},
						},
					},
					SecurityContext: &corev1.SecurityContext{
						Capabilities: &corev1.Capabilities{
							Add: []corev1.Capability{
								"SYS_PTRACE",
							},
						},
					},
				},
			},
		},
	},
}

var configMapWithoutTolerations = corev1.ConfigMap{
	Data: map[string]string{
		"cpu":                "4",
		"gpu":                "0",
		"gpu_accelerator":    "",
		"memory":             "8",
		"extended_resources": "{\"vpc.amazonaws.com/efa\": 32}",
		"name":               "head-node-template",
		"namespace":          "max",
		"memory_unit":        "Gi",
	},
}

var configMapWithTolerations = corev1.ConfigMap{
	Data: map[string]string{
		"cpu":                "4",
		"gpu":                "0",
		"gpu_accelerator":    "",
		"memory":             "8",
		"extended_resources": "{\"vpc.amazonaws.com/efa\": 32}",
		"name":               "head-node-template",
		"namespace":          "max",
		"tolerations":        "[{\"key\":\"blah1\",\"operator\":\"Exists\",\"effect\":\"NoExecute\"}]",
		"memory_unit":        "Gi",
	},
}

var configMapWithMemoryUnit = corev1.ConfigMap{
	Data: map[string]string{
		"cpu":                "4",
		"gpu":                "0",
		"gpu_accelerator":    "",
		"memory":             "8192",
		"extended_resources": "{\"vpc.amazonaws.com/efa\": 32}",
		"name":               "head-node-template",
		"namespace":          "max",
		"memory_unit":        "Mi",
	},
}

var workerSpecTest = rayv1api.WorkerGroupSpec{
	GroupName:   "",
	Replicas:    &workerReplicas,
	MinReplicas: &workerMinReplicas,
	MaxReplicas: &workerMaxReplicas,
	RayStartParams: map[string]string{
		"node-ip-address": "$MY_POD_IP",
	},
	Template: corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				"cni.projectcalico.org/containerID": "cce862a899455385e98e3453ba9ef5a376e85ad45c3e95b18e04e001204af728",
				"cni.projectcalico.org/podIP":       "172.17.60.2/32",
				"cni.projectcalico.org/podIPs":      "172.17.60.2/32",
				"openshift.io/scc":                  "restricted",
				"ray.io/ft-enabled":                 "false",
				"ray.io/health-state":               "",
				"custom":                            "value",
			},
			Labels: map[string]string{
				"app.kubernetes.io/created-by": "kuberay-operator",
				"app.kubernetes.io/name":       "kuberay",
				"ray.io/cluster":               "kuberay-cluster",
				"ray.io/cluster-dashboard":     "kuberay-cluster-dashboard",
				"ray.io/group":                 "8-CPUs",
				"ray.io/identifier":            "kuberay-cluster-worker",
				"ray.io/is-ray-node":           "yes",
				"ray.io/node-type":             "worker",
				"test":                         "value",
			},
			Name:      "kuberay-cluster-worker-8-cpus-4dp9v",
			Namespace: "max",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: "account",
			ImagePullSecrets: []corev1.LocalObjectReference{
				{Name: "foo"},
			},
			Tolerations: []corev1.Toleration{
				{
					Key:      "blah1",
					Operator: "Exists",
					Effect:   "NoExecute",
				},
			},
			Containers: []corev1.Container{
				{
					Name:            "ray-worker",
					Image:           "blublinsky1/ray310:2.5.0",
					ImagePullPolicy: "Always",
					Env: []corev1.EnvVar{
						{
							Name:  "AWS_KEY",
							Value: "123",
						},
						{
							Name:  "AWS_SECRET",
							Value: "1234",
						},
						{
							Name:  "RAY_DISABLE_DOCKER_CPU_WARNING",
							Value: "1",
						},
						{
							Name:  "TYPE",
							Value: "worker",
						},
						{
							Name:  "RAY_IP",
							Value: "kuberay-cluster-head-svc",
						},
						{
							Name:  "RAY_USAGE_STATS_KUBERAY_IN_USE",
							Value: "1",
						},
					},
					SecurityContext: &corev1.SecurityContext{
						Capabilities: &corev1.Capabilities{
							Add: []corev1.Capability{
								"SYS_PTRACE",
							},
						},
					},
				},
			},
		},
	},
}

var ClusterSpecTest = rayv1api.RayCluster{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "raycluster-sample",
		Namespace: "default",
		Annotations: map[string]string{
			"kubernetes.io/ingress.class": "nginx",
		},
	},
	Spec: rayv1api.RayClusterSpec{
		HeadGroupSpec: headSpecTest,
		WorkerGroupSpecs: []rayv1api.WorkerGroupSpec{
			workerSpecTest,
		},
	},
}

var ClusterSpecAutoscalerTest = rayv1api.RayCluster{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "raycluster-sample",
		Namespace: "default",
		Annotations: map[string]string{
			"kubernetes.io/ingress.class": "nginx",
		},
	},
	Spec: rayv1api.RayClusterSpec{
		HeadGroupSpec: headSpecTest,
		WorkerGroupSpecs: []rayv1api.WorkerGroupSpec{
			workerSpecTest,
		},
		EnableInTreeAutoscaling: new(true),
		AutoscalerOptions: &rayv1api.AutoscalerOptions{
			IdleTimeoutSeconds: new(int32(60)),
			UpscalingMode:      (*rayv1api.UpscalingMode)(new("Default")),
			ImagePullPolicy:    (*corev1.PullPolicy)(new("Always")),
			Resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceCPU:    resource.MustParse("500m"),
					corev1.ResourceMemory: resource.MustParse("512Mi"),
				},
			},
		},
	},
}

var JobNewClusterTest = rayv1api.RayJob{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test",
		Namespace: "test",
		Labels: map[string]string{
			"ray.io/user": "user",
		},
	},
	Spec: rayv1api.RayJobSpec{
		Entrypoint: "python /home/ray/samples/sample_code.py",
		Metadata: map[string]string{
			"job_submission_id": "123",
		},
		RuntimeEnvYAML:           "mytest yaml",
		ShutdownAfterJobFinishes: true,
		TTLSecondsAfterFinished:  secondsValue,
		RayClusterSpec:           &ClusterSpecTest.Spec,
	},
}

var JobExistingClusterTest = rayv1api.RayJob{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test",
		Namespace: "test",
		Labels: map[string]string{
			"ray.io/user": "user",
		},
	},
	Spec: rayv1api.RayJobSpec{
		Entrypoint:               "python /home/ray/samples/sample_code.py",
		RuntimeEnvYAML:           "mytest yaml",
		ShutdownAfterJobFinishes: true,
		TTLSecondsAfterFinished:  secondsValue,
		ClusterSelector: map[string]string{
			util.RayClusterUserLabelKey: "test",
		},
	},
}

var JobExistingClusterSubmitterTest = rayv1api.RayJob{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test",
		Namespace: "test",
		Labels: map[string]string{
			"ray.io/user": "user",
		},
	},
	Spec: rayv1api.RayJobSpec{
		Entrypoint:               "python /home/ray/samples/sample_code.py",
		RuntimeEnvYAML:           "mytest yaml",
		ShutdownAfterJobFinishes: true,
		TTLSecondsAfterFinished:  secondsValue,
		ClusterSelector: map[string]string{
			util.RayClusterUserLabelKey: "test",
		},
		SubmitterPodTemplate: &corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:  "test-submitter",
						Image: "image",
						Resources: corev1.ResourceRequirements{
							Limits: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("2"),
								corev1.ResourceMemory: resource.MustParse("1Gi"),
							},
							Requests: corev1.ResourceList{
								corev1.ResourceCPU:    resource.MustParse("500m"),
								corev1.ResourceMemory: resource.MustParse("200Mi"),
							},
						},
					},
				},
				RestartPolicy: corev1.RestartPolicyNever,
			},
		},
	},
}

var JobWithOutputTest = rayv1api.RayJob{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test",
		Namespace: "test",
		Labels: map[string]string{
			"ray.io/user": "user",
		},
	},
	Spec: rayv1api.RayJobSpec{
		Entrypoint:               "python /home/ray/samples/sample_code.py",
		RuntimeEnvYAML:           "mytest yaml",
		ShutdownAfterJobFinishes: true,
		TTLSecondsAfterFinished:  secondsValue,
		RayClusterSpec:           &ClusterSpecTest.Spec,
	},
	Status: rayv1api.RayJobStatus{
		JobStatus:           "RUNNING",
		JobDeploymentStatus: "Initializing",
		Message:             "Job is currently running",
		RayClusterName:      "raycluster-sample-xxxxx",
		StartTime:           &metav1.Time{Time: time.Date(2024, 0o7, 25, 0, 0, 0, 0, time.UTC)},
		EndTime:             nil,
	},
}

var ServiceV2Test = rayv1api.RayService{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "test",
		Namespace: "test",
		Labels: map[string]string{
			"ray.io/user": "user",
		},
	},
	Spec: rayv1api.RayServiceSpec{
		ServeConfigV2:                      "Some yaml value",
		RayClusterSpec:                     ClusterSpecTest.Spec,
		DeploymentUnhealthySecondThreshold: &unhealthySecondThreshold,
	},
}

var autoscalerOptions = &rayv1api.AutoscalerOptions{
	IdleTimeoutSeconds: new(int32(60)),
	UpscalingMode:      (*rayv1api.UpscalingMode)(new("Default")),
	Image:              new("Some Image"),
	ImagePullPolicy:    (*corev1.PullPolicy)(new("Always")),
	Env: []corev1.EnvVar{
		{
			Name:  "n1",
			Value: "v1",
		},
	},
	EnvFrom: []corev1.EnvFromSource{
		{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "ConfigMap",
				},
			},
		},
		{
			SecretRef: &corev1.SecretEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "Secret",
				},
			},
		},
	},
	VolumeMounts: []corev1.VolumeMount{
		{
			Name:             "vmount1",
			MountPath:        "path1",
			ReadOnly:         false,
			MountPropagation: (*corev1.MountPropagationMode)(new("None")),
		},
		{
			Name:             "vmount2",
			MountPath:        "path2",
			ReadOnly:         true,
			MountPropagation: (*corev1.MountPropagationMode)(new("HostToContainer")),
		},
	},
	Resources: &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	},
}

var expectedAnnotations = map[string]string{
	"custom": "value",
}

var expectedLabels = map[string]string{
	"test": "value",
}

var expectedHeadEnv = &api.EnvironmentVariables{
	Values: map[string]string{
		"AWS_KEY": "123",
	},
	ValuesFrom: map[string]*api.EnvValueFrom{
		"REDIS_PASSWORD": {
			Source: api.EnvValueFrom_SECRET,
			Name:   "redis-password-secret",
			Key:    "password",
		},
		"CONFIGMAP": {
			Source: api.EnvValueFrom_CONFIGMAP,
			Name:   "special-config",
			Key:    "special.how",
		},
		"ResourceFieldRef": {
			Source: api.EnvValueFrom_RESOURCEFIELD,
			Name:   "my-container",
			Key:    "resource",
		},
		"FieldRef": {
			Source: api.EnvValueFrom_FIELD,
			Key:    "path",
		},
	},
}

var expectedEnv = &api.EnvironmentVariables{
	Values: map[string]string{
		"AWS_KEY":    "123",
		"AWS_SECRET": "1234",
	},
	ValuesFrom: map[string]*api.EnvValueFrom{},
}

var expectedTolerations = api.PodToleration{
	Key:      "blah1",
	Operator: "Exists",
	Effect:   "NoExecute",
}

func TestPopulateHeadNodeSpec(t *testing.T) {
	groupSpec := PopulateHeadNodeSpec(headSpecTest)

	if groupSpec.ServiceAccount != "account" {
		t.Errorf("failed to convert service account")
	}
	if groupSpec.EnableIngress != *headSpecTest.EnableIngress {
		t.Errorf("failed to convert enableIngress")
	}
	if groupSpec.ImagePullSecret != "foo" {
		t.Errorf("failed to convert image pull secret")
	}
	if groupSpec.ImagePullPolicy != "Always" {
		t.Errorf("failed to convert image pull policy")
	}
	if !reflect.DeepEqual(groupSpec.Annotations, expectedAnnotations) {
		t.Errorf("failed to convert annotations, got %v, expected %v", groupSpec.Annotations, expectedAnnotations)
	}
	if !reflect.DeepEqual(groupSpec.Labels, expectedLabels) {
		t.Errorf("failed to convert labels, got %v, expected %v", groupSpec.Labels, expectedLabels)
	}
	if !reflect.DeepEqual(groupSpec.Environment, expectedHeadEnv) {
		t.Errorf("failed to convert environment, got %v, expected %v", groupSpec.Environment, expectedHeadEnv)
	}
	// Cannot use deep equal since protobuf locks copying
	if groupSpec.SecurityContext == nil || groupSpec.SecurityContext.Capabilities == nil || len(groupSpec.SecurityContext.Capabilities.Add) != 1 {
		t.Errorf("failed to convert security context")
	}
}

func TestPopulateWorkerNodeSpec(t *testing.T) {
	groupSpec := PopulateWorkerNodeSpec([]rayv1api.WorkerGroupSpec{workerSpecTest})[0]

	if groupSpec.ServiceAccount != "account" {
		t.Errorf("failed to convert service account")
	}
	if groupSpec.ImagePullSecret != "foo" {
		t.Errorf("failed to convert image pull secret")
	}
	if groupSpec.ImagePullPolicy != "Always" {
		t.Errorf("failed to convert image pull policy")
	}
	if !reflect.DeepEqual(groupSpec.Annotations, expectedAnnotations) {
		t.Errorf("failed to convert annotations, got %v, expected %v", groupSpec.Annotations, expectedAnnotations)
	}
	if !reflect.DeepEqual(groupSpec.Labels, expectedLabels) {
		t.Errorf("failed to convert labels, got %v, expected %v", groupSpec.Labels, expectedLabels)
	}
	if !reflect.DeepEqual(groupSpec.Environment, expectedEnv) {
		t.Errorf("failed to convert environment, got %v, expected %v", groupSpec.Environment, expectedEnv)
	}
	if groupSpec.SecurityContext == nil || groupSpec.SecurityContext.Capabilities == nil || len(groupSpec.SecurityContext.Capabilities.Add) != 1 {
		t.Errorf("failed to convert security context")
	}
}

func TestAutoscalerOptions(t *testing.T) {
	options := convertAutoscalingOptions(autoscalerOptions)
	assert.Equal(t, int32(60), options.IdleTimeoutSeconds)
	assert.Equal(t, "Default", options.UpscalingMode)
	assert.Equal(t, "Some Image", options.Image)
	assert.Equal(t, "Always", options.ImagePullPolicy)
	assert.Equal(t, "500m", options.Cpu)
	assert.Equal(t, "512Mi", options.Memory)
	assert.Len(t, options.Envs.Values, 1)
	assert.Len(t, options.Envs.ValuesFrom, 2)
	assert.Len(t, options.Volumes, 2)
}

func TestNilAutoscalerOptions(t *testing.T) {
	options := convertAutoscalingOptions(nil)
	assert.Nil(t, options)
}

func TestFromCrdToAPIClusters(t *testing.T) {
	clusters := []*rayv1api.RayCluster{&ClusterSpecTest}
	clusterEventsMap := map[string][]eventsv1.Event{}
	apiClusters := FromCrdToAPIClusters(clusters, clusterEventsMap)
	assert.Len(t, apiClusters, 1)

	apiCluster := apiClusters[0]
	if len(apiCluster.Annotations) != 1 {
		t.Errorf("failed to convert cluster's annotations")
	}
	assert.Equal(t, "nginx", apiCluster.Annotations["kubernetes.io/ingress.class"])
}

func TestPopulateRayClusterSpec(t *testing.T) {
	cluster := FromCrdToAPICluster(&ClusterSpecTest, []eventsv1.Event{})
	if len(cluster.Annotations) != 1 {
		t.Errorf("failed to convert cluster's annotations")
	}
	assert.False(t, cluster.ClusterSpec.EnableInTreeAutoscaling)
	if cluster.ClusterSpec.AutoscalerOptions != nil {
		t.Errorf("unexpected autoscaler annotations")
	}
	cluster = FromCrdToAPICluster(&ClusterSpecAutoscalerTest, []eventsv1.Event{})
	assert.True(t, cluster.ClusterSpec.EnableInTreeAutoscaling)
	if cluster.ClusterSpec.AutoscalerOptions == nil {
		t.Errorf("autoscaler annotations not found")
	}
	assert.Equal(t, int32(60), cluster.ClusterSpec.AutoscalerOptions.IdleTimeoutSeconds)
	assert.Equal(t, "Default", cluster.ClusterSpec.AutoscalerOptions.UpscalingMode)
	assert.Equal(t, "Always", cluster.ClusterSpec.AutoscalerOptions.ImagePullPolicy)
	assert.Equal(t, "500m", cluster.ClusterSpec.AutoscalerOptions.Cpu)
	assert.Equal(t, "512Mi", cluster.ClusterSpec.AutoscalerOptions.Memory)
	assert.Len(t, cluster.ClusterSpec.HeadGroupSpec.Environment.ValuesFrom, 4)
	for name, value := range cluster.ClusterSpec.HeadGroupSpec.Environment.ValuesFrom {
		switch name {
		case "REDIS_PASSWORD":
			assert.Equal(t, "SECRET", value.Source.String())
			assert.Equal(t, "redis-password-secret", value.Name)
			assert.Equal(t, "password", value.Key)
		case "CONFIGMAP":
			assert.Equal(t, "CONFIGMAP", value.Source.String())
			assert.Equal(t, "special-config", value.Name)
			assert.Equal(t, "special.how", value.Key)
		case "ResourceFieldRef":
			assert.Equal(t, "RESOURCEFIELD", value.Source.String())
			assert.Equal(t, "my-container", value.Name)
			assert.Equal(t, "resource", value.Key)
		default:
			assert.Equal(t, "FIELD", value.Source.String())
			assert.Empty(t, value.Name)
			assert.Equal(t, "path", value.Key)
		}
	}
}

func TestFromKubeToAPIComputeTemplates(t *testing.T) {
	configMaps := []*corev1.ConfigMap{&configMapWithoutTolerations}
	templates := FromKubeToAPIComputeTemplates(configMaps)
	assert.Len(t, templates, 1)

	template := templates[0]
	assert.Equal(t, uint32(4), template.Cpu, "CPU mismatch")
	assert.Equal(t, uint32(8), template.Memory, "Memory mismatch")
	assert.Equal(t, uint32(0), template.Gpu, "GPU mismatch")
	assert.Empty(t, template.GpuAccelerator, "GPU accelerator mismatch")
}

func TestPopulateTemplate(t *testing.T) {
	template := FromKubeToAPIComputeTemplate(&configMapWithoutTolerations)
	if len(template.Tolerations) != 0 {
		t.Errorf("failed to convert config map, expected no tolerations, got %d", len(template.Tolerations))
	}

	template = FromKubeToAPIComputeTemplate(&configMapWithTolerations)
	if len(template.Tolerations) != 1 {
		t.Errorf("failed to convert config map, expected 1 toleration, got %d", len(template.Tolerations))
	}
	if template.Tolerations[0].Key != expectedTolerations.Key ||
		template.Tolerations[0].Operator != expectedTolerations.Operator ||
		template.Tolerations[0].Effect != expectedTolerations.Effect {
		t.Errorf("failed to convert config map, got %v, expected %v", tolerationToString(template.Tolerations[0]),
			tolerationToString(&expectedTolerations))
	}

	assert.Equal(t, uint32(4), template.Cpu, "CPU mismatch")
	assert.Equal(t, uint32(8), template.Memory, "Memory mismatch")
	assert.Equal(t, uint32(0), template.Gpu, "GPU mismatch")
	assert.Equal(
		t,
		map[string]uint32{"vpc.amazonaws.com/efa": 32},
		template.ExtendedResources,
		"Extended resources mismatch",
	)

	template = FromKubeToAPIComputeTemplate(&configMapWithMemoryUnit)
	assert.Equal(t, uint32(8192), template.Memory, "Memory mismatch")
	// test memory unit MiB
	assert.Equal(t, "Mi", template.MemoryUnit, "Memory Unit mismatch")
}

func tolerationToString(toleration *api.PodToleration) string {
	return "Key: " + toleration.Key + " Operator: " + toleration.Operator + " Effect: " + toleration.Effect
}

func TestFromCrdToAPIJobs(t *testing.T) {
	jobs := []*rayv1api.RayJob{&JobNewClusterTest}
	apiJobs := FromCrdToAPIJobs(jobs)
	assert.Len(t, apiJobs, 1)

	apiJob := apiJobs[0]
	assert.Equal(t, "test", apiJob.Name)
	assert.Equal(t, "test", apiJob.Namespace)
	assert.Equal(t, "user", apiJob.User)
	assert.Equal(t, "python /home/ray/samples/sample_code.py", apiJob.Entrypoint)
	assert.Equal(t, "123", apiJob.Metadata["job_submission_id"])
}

func TestPopulateJob(t *testing.T) {
	job := FromCrdToAPIJob(&JobNewClusterTest)
	fmt.Printf("jobWithCluster = %#v\n", job)
	assert.Equal(t, "test", job.Name)
	assert.Equal(t, "test", job.Namespace)
	assert.Equal(t, "user", job.User)
	assert.Greater(t, len(job.RuntimeEnv), 1)
	assert.Nil(t, job.ClusterSelector)
	assert.NotNil(t, job.ClusterSpec)

	job = FromCrdToAPIJob(&JobExistingClusterTest)
	fmt.Printf("jobReferenceCluster = %#v\n", job)
	assert.Equal(t, "test", job.Name)
	assert.Equal(t, "test", job.Namespace)
	assert.Equal(t, "user", job.User)
	assert.Greater(t, len(job.RuntimeEnv), 1)
	assert.NotNil(t, job.ClusterSelector)
	assert.Nil(t, job.ClusterSpec)

	job = FromCrdToAPIJob(&JobExistingClusterSubmitterTest)
	fmt.Printf("jobReferenceCluster = %#v\n", job)
	assert.Equal(t, "test", job.Name)
	assert.Equal(t, "test", job.Namespace)
	assert.Equal(t, "user", job.User)
	assert.Greater(t, len(job.RuntimeEnv), 1)
	assert.NotNil(t, job.ClusterSelector)
	assert.Nil(t, job.ClusterSpec)
	assert.Equal(t, "image", job.JobSubmitter.Image)
	assert.Equal(t, "2", job.JobSubmitter.Cpu)

	job = FromCrdToAPIJob(&JobWithOutputTest)
	fmt.Printf("jobWithOutput = %#v\n", job)
	assert.Equal(t, time.Date(2024, 0o7, 25, 0, 0, 0, 0, time.UTC), job.StartTime.AsTime())
	assert.Nil(t, job.EndTime)
	assert.Equal(t, "RUNNING", job.JobStatus)
	assert.Equal(t, "Initializing", job.JobDeploymentStatus)
	assert.Equal(t, "Job is currently running", job.Message)
	assert.Equal(t, "raycluster-sample-xxxxx", job.RayClusterName)
}

func TestFromCrdToAPIServices(t *testing.T) {
	services := []*rayv1api.RayService{&ServiceV2Test}
	serviceEventsMap := map[string][]eventsv1.Event{}
	apiServices := FromCrdToAPIServices(services, serviceEventsMap)
	assert.Len(t, apiServices, 1)

	apiService := apiServices[0]
	assert.Equal(t, ServiceV2Test.ObjectMeta.Name, apiService.Name)
	assert.Equal(t, ServiceV2Test.ObjectMeta.Namespace, apiService.Namespace)
	assert.Equal(t, ServiceV2Test.ObjectMeta.Labels["ray.io/user"], apiService.User)
}

func TestPopulateService(t *testing.T) {
	service := FromCrdToAPIService(&ServiceV2Test, []eventsv1.Event{})
	assert.Equal(t, ServiceV2Test.ObjectMeta.Name, service.Name)
	assert.Equal(t, ServiceV2Test.ObjectMeta.Namespace, service.Namespace)
	assert.Equal(t, ServiceV2Test.ObjectMeta.Labels["ray.io/user"], service.User)
	assert.Equal(t, ServiceV2Test.Spec.ServeConfigV2, service.ServeConfig_V2)
	assert.Equal(t, int64(-1), service.DeleteAt.Seconds)
}

func TestPopulateServeApplicationStatus(t *testing.T) {
	expectedAppName := "app0"
	expectedAppStatus := rayv1api.AppStatus{
		Deployments: map[string]rayv1api.ServeDeploymentStatus{},
		Status:      rayv1api.ApplicationStatusEnum.DEPLOYING,
		Message:     "Deploying...",
	}
	serveApplicationStatuses := map[string]rayv1api.AppStatus{
		expectedAppName: expectedAppStatus,
	}
	appStatuses := PopulateServeApplicationStatus(serveApplicationStatuses)
	assert.Len(t, appStatuses, 1)

	appStatus := appStatuses[0]
	assert.Equal(t, expectedAppName, appStatus.Name)
	assert.Equal(t, expectedAppStatus.Status, appStatus.Status)
	assert.Equal(t, expectedAppStatus.Message, appStatus.Message)
}

func TestPopulateServeDeploymentStatus(t *testing.T) {
	expectedDeploymentStatus := rayv1api.ServeDeploymentStatus{
		Status:  rayv1api.DeploymentStatusEnum.UPDATING,
		Message: "Updating...",
	}
	serveDeploymentStatuses := map[string]rayv1api.ServeDeploymentStatus{
		"deployment0": expectedDeploymentStatus,
	}
	deploymentStatuses := PopulateServeDeploymentStatus(serveDeploymentStatuses)
	assert.Len(t, deploymentStatuses, 1)

	deploymentStatus := deploymentStatuses[0]
	assert.Equal(t, expectedDeploymentStatus.Status, deploymentStatus.Status)
	assert.Equal(t, expectedDeploymentStatus.Message, deploymentStatus.Message)
}

func TestPopulateRayServiceEvent(t *testing.T) {
	expectedServiceName := "svc0"
	expectedEvent := eventsv1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
		},
		Reason:          "test",
		Note:            "test",
		Type:            "Normal",
		DeprecatedCount: 2,
	}
	events := []eventsv1.Event{expectedEvent}
	serviceEvents := PopulateRayServiceEvent(expectedServiceName, events)
	assert.Len(t, serviceEvents, 1)

	serviceEvent := serviceEvents[0]
	assert.Equal(t, expectedEvent.Name, serviceEvent.Id)
	assert.Equal(t, expectedServiceName+"-"+expectedEvent.ObjectMeta.Name, serviceEvent.Name)
	assert.Equal(t, expectedEvent.Reason, serviceEvent.Reason)
	assert.Equal(t, expectedEvent.Note, serviceEvent.Message)
	assert.Equal(t, expectedEvent.Type, serviceEvent.Type)
	assert.Equal(t, expectedEvent.DeprecatedCount, serviceEvent.Count)
}
