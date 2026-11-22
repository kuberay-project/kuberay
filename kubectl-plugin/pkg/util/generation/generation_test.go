package generation

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1 "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/utils/ptr"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
)

func TestGenerateRayClusterApplyConfig(t *testing.T) {
	labels := map[string]string{
		"blue":    "jay",
		"eastern": "bluebird",
	}
	annotations := map[string]string{
		"mourning":  "dove",
		"baltimore": "oriole",
	}

	testRayClusterConfig := RayClusterConfig{
		Name:        new("test-ray-cluster"),
		Namespace:   new("default"),
		Labels:      labels,
		Annotations: annotations,
		RayVersion:  ptr.To(util.RayVersion),
		Image:       ptr.To(util.RayImage),
		Head: &Head{
			CPU:    new("1"),
			Memory: new("5Gi"),
			GPU:    new("1"),
			RayStartParams: map[string]string{
				"dashboard-host": "1.2.3.4",
				"num-cpus":       "0",
			},
		},
		WorkerGroups: []WorkerGroup{
			{
				Replicas:   int32(3),
				NumOfHosts: new(int32(2)),
				CPU:        new("2"),
				Memory:     new("10Gi"),
				GPU:        new("1"),
				TPU:        new("1"),
				RayStartParams: map[string]string{
					"dagon":    "azathoth",
					"shoggoth": "cthulhu",
				},
				NodeSelectors: map[string]string{
					"app": "ray",
					"env": "dev",
				},
			},
		},
		Autoscaler: &Autoscaler{
			Version: AutoscalerV2,
		},
	}

	result := testRayClusterConfig.GenerateRayClusterApplyConfig()

	expected := rayv1ac.RayClusterApplyConfiguration{
		TypeMetaApplyConfiguration: metav1.TypeMetaApplyConfiguration{
			APIVersion: new("ray.io/v1"),
			Kind:       new("RayCluster"),
		},
		ObjectMetaApplyConfiguration: &metav1.ObjectMetaApplyConfiguration{
			Name:        new("test-ray-cluster"),
			Namespace:   new("default"),
			Labels:      labels,
			Annotations: annotations,
		},
		Spec: &rayv1ac.RayClusterSpecApplyConfiguration{
			EnableInTreeAutoscaling: new(true),
			AutoscalerOptions: &rayv1ac.AutoscalerOptionsApplyConfiguration{
				Version: ptr.To(rayv1.AutoscalerVersionV2),
			},
			RayVersion: ptr.To(util.RayVersion),
			HeadGroupSpec: &rayv1ac.HeadGroupSpecApplyConfiguration{
				RayStartParams: map[string]string{"dashboard-host": "1.2.3.4", "num-cpus": "0"},
				Template: &corev1ac.PodTemplateSpecApplyConfiguration{
					Spec: &corev1ac.PodSpecApplyConfiguration{
						Containers: []corev1ac.ContainerApplyConfiguration{
							{
								Name:  new("ray-head"),
								Image: ptr.To(util.RayImage),
								Resources: &corev1ac.ResourceRequirementsApplyConfiguration{
									Requests: &corev1.ResourceList{
										corev1.ResourceCPU:                          resource.MustParse("1"),
										corev1.ResourceMemory:                       resource.MustParse("5Gi"),
										corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
									},
									Limits: &corev1.ResourceList{
										corev1.ResourceMemory:                       resource.MustParse("5Gi"),
										corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
									},
								},
								Ports: []corev1ac.ContainerPortApplyConfiguration{
									{
										ContainerPort: new(int32(6379)),
										Name:          new("gcs-server"),
									},
									{
										ContainerPort: new(int32(8265)),
										Name:          new("dashboard"),
									},
									{
										ContainerPort: new(int32(10001)),
										Name:          new("client"),
									},
								},
							},
						},
					},
				},
			},
			WorkerGroupSpecs: []rayv1ac.WorkerGroupSpecApplyConfiguration{
				{
					GroupName:      new("default-group"),
					Replicas:       new(int32(3)),
					NumOfHosts:     new(int32(2)),
					RayStartParams: map[string]string{"dagon": "azathoth", "shoggoth": "cthulhu"},
					Template: &corev1ac.PodTemplateSpecApplyConfiguration{
						Spec: &corev1ac.PodSpecApplyConfiguration{
							Containers: []corev1ac.ContainerApplyConfiguration{
								{
									Name:  new("ray-worker"),
									Image: ptr.To(util.RayImage),
									Resources: &corev1ac.ResourceRequirementsApplyConfiguration{
										Requests: &corev1.ResourceList{
											corev1.ResourceCPU:                          resource.MustParse("2"),
											corev1.ResourceMemory:                       resource.MustParse("10Gi"),
											corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
											corev1.ResourceName(util.ResourceGoogleTPU): resource.MustParse("1"),
										},
										Limits: &corev1.ResourceList{
											corev1.ResourceMemory:                       resource.MustParse("10Gi"),
											corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
											corev1.ResourceName(util.ResourceGoogleTPU): resource.MustParse("1"),
										},
									},
								},
							},
							NodeSelector: map[string]string{"app": "ray", "env": "dev"},
						},
					},
				},
			},
		},
	}

	require.Equal(t, &expected, result)
}

func TestGenerateRayJobApplyConfig(t *testing.T) {
	testRayJobYamlObject := RayJobYamlObject{
		RayJobName:               "test-ray-job",
		Namespace:                "default",
		SubmissionMode:           "InteractiveMode",
		TTLSecondsAfterFinished:  100,
		ShutdownAfterJobFinishes: true,
		RayClusterConfig: RayClusterConfig{
			RayVersion: ptr.To(util.RayVersion),
			Image:      ptr.To(util.RayImage),
			Head: &Head{
				CPU:    new("1"),
				GPU:    new("1"),
				Memory: new("5Gi"),
			},
			WorkerGroups: []WorkerGroup{
				{
					Replicas:   int32(3),
					NumOfHosts: new(int32(2)),
					CPU:        new("2"),
					Memory:     new("10Gi"),
					GPU:        new("0"),
					TPU:        new("0"),
					RayStartParams: map[string]string{
						"dagon":    "azathoth",
						"shoggoth": "cthulhu",
					},
				},
			},
		},
	}

	result := testRayJobYamlObject.GenerateRayJobApplyConfig()

	expected := rayv1ac.RayJobApplyConfiguration{
		TypeMetaApplyConfiguration: metav1.TypeMetaApplyConfiguration{
			APIVersion: new("ray.io/v1"),
			Kind:       new("RayJob"),
		},
		ObjectMetaApplyConfiguration: &metav1.ObjectMetaApplyConfiguration{
			Name:      new("test-ray-job"),
			Namespace: new("default"),
		},
		Spec: &rayv1ac.RayJobSpecApplyConfiguration{
			SubmissionMode:           new(rayv1.JobSubmissionMode(testRayJobYamlObject.SubmissionMode)),
			Entrypoint:               new(""),
			TTLSecondsAfterFinished:  new(int32(100)),
			ShutdownAfterJobFinishes: new(true),
			RayClusterSpec: &rayv1ac.RayClusterSpecApplyConfiguration{
				RayVersion: ptr.To(util.RayVersion),
				HeadGroupSpec: &rayv1ac.HeadGroupSpecApplyConfiguration{
					Template: &corev1ac.PodTemplateSpecApplyConfiguration{
						Spec: &corev1ac.PodSpecApplyConfiguration{
							Containers: []corev1ac.ContainerApplyConfiguration{
								{
									Name:  new("ray-head"),
									Image: ptr.To(util.RayImage),
									Resources: &corev1ac.ResourceRequirementsApplyConfiguration{
										Requests: &corev1.ResourceList{
											corev1.ResourceCPU:                          resource.MustParse("1"),
											corev1.ResourceMemory:                       resource.MustParse("5Gi"),
											corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
										},
										Limits: &corev1.ResourceList{
											corev1.ResourceMemory:                       resource.MustParse("5Gi"),
											corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
										},
									},
									Ports: []corev1ac.ContainerPortApplyConfiguration{
										{
											ContainerPort: new(int32(6379)),
											Name:          new("gcs-server"),
										},
										{
											ContainerPort: new(int32(8265)),
											Name:          new("dashboard"),
										},
										{
											ContainerPort: new(int32(10001)),
											Name:          new("client"),
										},
									},
								},
							},
						},
					},
				},
				WorkerGroupSpecs: []rayv1ac.WorkerGroupSpecApplyConfiguration{
					{
						GroupName:      new("default-group"),
						Replicas:       new(int32(3)),
						NumOfHosts:     new(int32(2)),
						RayStartParams: map[string]string{"dagon": "azathoth", "shoggoth": "cthulhu"},
						Template: &corev1ac.PodTemplateSpecApplyConfiguration{
							Spec: &corev1ac.PodSpecApplyConfiguration{
								Containers: []corev1ac.ContainerApplyConfiguration{
									{
										Name:  new("ray-worker"),
										Image: ptr.To(util.RayImage),
										Resources: &corev1ac.ResourceRequirementsApplyConfiguration{
											Requests: &corev1.ResourceList{
												corev1.ResourceCPU:    resource.MustParse("2"),
												corev1.ResourceMemory: resource.MustParse("10Gi"),
											},
											Limits: &corev1.ResourceList{
												corev1.ResourceMemory: resource.MustParse("10Gi"),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	require.Equal(t, &expected, result)
}

func TestConvertRayClusterApplyConfigToYaml(t *testing.T) {
	testRayClusterConfig := RayClusterConfig{
		Name:      new("test-ray-cluster"),
		Namespace: new("default"),
		Labels: map[string]string{
			"purple":     "finch",
			"red-tailed": "hawk",
		},
		Annotations: map[string]string{
			"american": "goldfinch",
			"piping":   "plover",
		},
		Autoscaler: &Autoscaler{
			Version: AutoscalerV1,
		},
		RayVersion: ptr.To(util.RayVersion),
		Image:      ptr.To(util.RayImage),
		Head: &Head{
			CPU:    new("1"),
			Memory: new("5Gi"),
			GPU:    new("1"),
			RayStartParams: map[string]string{
				"num-cpus": "0",
			},
		},
		WorkerGroups: []WorkerGroup{
			{
				Replicas:   int32(3),
				NumOfHosts: new(int32(2)),
				CPU:        new("2"),
				Memory:     new("10Gi"),
				GPU:        new("0"),
				TPU:        new("0"),
			},
		},
	}

	result := testRayClusterConfig.GenerateRayClusterApplyConfig()

	resultString, err := ConvertRayClusterApplyConfigToYaml(result)
	require.NoError(t, err)
	expectedResultYaml := fmt.Sprintf(`apiVersion: ray.io/v1
kind: RayCluster
metadata:
  annotations:
    american: goldfinch
    piping: plover
  labels:
    purple: finch
    red-tailed: hawk
  name: test-ray-cluster
  namespace: default
spec:
  enableInTreeAutoscaling: true
  autoscalerOptions:
    version: v1
  headGroupSpec:
    rayStartParams:
      num-cpus: "0"
    template:
      spec:
        containers:
        - image: %s
          name: ray-head
          ports:
          - containerPort: 6379
            name: gcs-server
          - containerPort: 8265
            name: dashboard
          - containerPort: 10001
            name: client
          resources:
            limits:
              memory: 5Gi
              nvidia.com/gpu: "1"
            requests:
              cpu: "1"
              memory: 5Gi
              nvidia.com/gpu: "1"
  rayVersion: %s
  workerGroupSpecs:
  - groupName: default-group
    replicas: 3
    numOfHosts: 2
    template:
      spec:
        containers:
        - image: %s
          name: ray-worker
          resources:
            limits:
              memory: 10Gi
            requests:
              cpu: "2"
              memory: 10Gi`, util.RayImage, util.RayVersion, util.RayImage)

	assert.YAMLEq(t, expectedResultYaml, resultString)
}

func TestGenerateResources(t *testing.T) {
	tests := []struct {
		expectedRequestResources corev1.ResourceList
		expectedLimitResources   corev1.ResourceList
		cpu                      *string
		memory                   *string
		ephemeralStorage         *string
		gpu                      *string
		tpu                      *string
		name                     string
	}{
		{
			name:             "should generate resources with CPU, memory, ephemeral storage, and GPU",
			cpu:              new("1"),
			memory:           new("5Gi"),
			ephemeralStorage: new("10Gi"),
			gpu:              new("1"),
			tpu:              new("0"),
			expectedRequestResources: corev1.ResourceList{
				corev1.ResourceCPU:                          resource.MustParse("1"),
				corev1.ResourceMemory:                       resource.MustParse("5Gi"),
				corev1.ResourceEphemeralStorage:             resource.MustParse("10Gi"),
				corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
			},
			expectedLimitResources: corev1.ResourceList{
				corev1.ResourceMemory:                       resource.MustParse("5Gi"),
				corev1.ResourceEphemeralStorage:             resource.MustParse("10Gi"),
				corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
			},
		},
		{
			name:   "should only generate resources with CPU and memory if ephemeral storage isn't set and GPUs are 0",
			cpu:    new("1"),
			memory: new("5Gi"),
			expectedRequestResources: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("1"),
				corev1.ResourceMemory: resource.MustParse("5Gi"),
			},
			expectedLimitResources: corev1.ResourceList{
				corev1.ResourceMemory: resource.MustParse("5Gi"),
			},
		},
		{
			name:             "should generate resources with CPU, memory, ephemeral storage, and TPU",
			cpu:              new("1"),
			memory:           new("5Gi"),
			ephemeralStorage: new("10Gi"),
			gpu:              new("0"),
			tpu:              new("4"),
			expectedRequestResources: corev1.ResourceList{
				corev1.ResourceCPU:                          resource.MustParse("1"),
				corev1.ResourceMemory:                       resource.MustParse("5Gi"),
				corev1.ResourceEphemeralStorage:             resource.MustParse("10Gi"),
				corev1.ResourceName(util.ResourceGoogleTPU): resource.MustParse("4"),
			},
			expectedLimitResources: corev1.ResourceList{
				corev1.ResourceMemory:                       resource.MustParse("5Gi"),
				corev1.ResourceEphemeralStorage:             resource.MustParse("10Gi"),
				corev1.ResourceName(util.ResourceGoogleTPU): resource.MustParse("4"),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.expectedRequestResources, generateRequestResources(test.cpu, test.memory, test.ephemeralStorage, test.gpu, test.tpu))
			assert.Equal(t, test.expectedLimitResources, generateLimitResources(test.memory, test.ephemeralStorage, test.gpu, test.tpu))
		})
	}
}

func TestGenerateRayClusterSpec(t *testing.T) {
	testRayClusterConfig := RayClusterConfig{
		Autoscaler: &Autoscaler{
			Version: AutoscalerV2,
		},
		RayVersion:     new("1.2.3"),
		Image:          new("rayproject/ray:2.52.0"),
		ServiceAccount: new("my-service-account"),
		Head: &Head{
			CPU:              new("1"),
			Memory:           new("5Gi"),
			GPU:              new("1"),
			EphemeralStorage: new("10Gi"),
			RayStartParams: map[string]string{
				"softmax": "GELU",
			},
			NodeSelectors: map[string]string{
				"head-selector1": "foo",
				"head-selector2": "bar",
			},
		},
		WorkerGroups: []WorkerGroup{
			{
				Replicas:   int32(3),
				NumOfHosts: new(int32(1)),
				CPU:        new("2"),
				Memory:     new("10Gi"),
				GPU:        new("0"),
				TPU:        new("0"),
				RayStartParams: map[string]string{
					"dagon":    "azathoth",
					"shoggoth": "cthulhu",
				},
				NodeSelectors: map[string]string{
					"worker-selector1": "baz",
					"worker-selector2": "qux",
				},
			},
			{
				Name: new("worker-group-2"),
				GPU:  new("1"),
			},
		},
	}

	expected := &rayv1ac.RayClusterSpecApplyConfiguration{
		EnableInTreeAutoscaling: new(true),
		AutoscalerOptions: &rayv1ac.AutoscalerOptionsApplyConfiguration{
			Version: ptr.To(rayv1.AutoscalerVersionV2),
		},
		RayVersion: new("1.2.3"),
		HeadGroupSpec: &rayv1ac.HeadGroupSpecApplyConfiguration{
			RayStartParams: map[string]string{"softmax": "GELU"},
			Template: &corev1ac.PodTemplateSpecApplyConfiguration{
				Spec: &corev1ac.PodSpecApplyConfiguration{
					ServiceAccountName: new("my-service-account"),
					Containers: []corev1ac.ContainerApplyConfiguration{
						{
							Name:  new("ray-head"),
							Image: new("rayproject/ray:2.52.0"),
							Resources: &corev1ac.ResourceRequirementsApplyConfiguration{
								Requests: &corev1.ResourceList{
									corev1.ResourceCPU:                          resource.MustParse("1"),
									corev1.ResourceMemory:                       resource.MustParse("5Gi"),
									corev1.ResourceEphemeralStorage:             resource.MustParse("10Gi"),
									corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
								},
								Limits: &corev1.ResourceList{
									corev1.ResourceMemory:                       resource.MustParse("5Gi"),
									corev1.ResourceEphemeralStorage:             resource.MustParse("10Gi"),
									corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
								},
							},
							Ports: []corev1ac.ContainerPortApplyConfiguration{
								{
									ContainerPort: new(int32(6379)),
									Name:          new("gcs-server"),
								},
								{
									ContainerPort: new(int32(8265)),
									Name:          new("dashboard"),
								},
								{
									ContainerPort: new(int32(10001)),
									Name:          new("client"),
								},
							},
						},
					},
					NodeSelector: map[string]string{
						"head-selector1": "foo",
						"head-selector2": "bar",
					},
				},
			},
		},
		WorkerGroupSpecs: []rayv1ac.WorkerGroupSpecApplyConfiguration{
			{
				GroupName:  new("default-group"),
				NumOfHosts: new(int32(1)),
				Replicas:   new(int32(3)),
				RayStartParams: map[string]string{
					"dagon":    "azathoth",
					"shoggoth": "cthulhu",
				},
				Template: &corev1ac.PodTemplateSpecApplyConfiguration{
					Spec: &corev1ac.PodSpecApplyConfiguration{
						ServiceAccountName: new("my-service-account"),
						Containers: []corev1ac.ContainerApplyConfiguration{
							{
								Name:  new("ray-worker"),
								Image: new("rayproject/ray:2.52.0"),
								Resources: &corev1ac.ResourceRequirementsApplyConfiguration{
									Requests: &corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("2"),
										corev1.ResourceMemory: resource.MustParse("10Gi"),
									},
									Limits: &corev1.ResourceList{
										corev1.ResourceMemory: resource.MustParse("10Gi"),
									},
								},
							},
						},
						NodeSelector: map[string]string{
							"worker-selector1": "baz",
							"worker-selector2": "qux",
						},
					},
				},
			},
			{
				GroupName: new("worker-group-2"),
				Replicas:  new(int32(0)),
				Template: &corev1ac.PodTemplateSpecApplyConfiguration{
					Spec: &corev1ac.PodSpecApplyConfiguration{
						ServiceAccountName: new("my-service-account"),
						Containers: []corev1ac.ContainerApplyConfiguration{
							{
								Name:  new("ray-worker"),
								Image: new("rayproject/ray:2.52.0"),
								Resources: &corev1ac.ResourceRequirementsApplyConfiguration{
									Requests: &corev1.ResourceList{
										corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
									},
									Limits: &corev1.ResourceList{
										corev1.ResourceName(util.ResourceNvidiaGPU): resource.MustParse("1"),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	result := testRayClusterConfig.generateRayClusterSpec()

	assert.Equal(t, expected, result)
}

func TestSetGCSFuseOptions(t *testing.T) {
	gcsFuse := &GCSFuse{
		BucketName:   "my-bucket",
		MountPath:    "/mnt/my-data",
		MountOptions: new("uid=1234,gid=5678"),
		Resources: &GCSFuseResources{
			CPU:              new("1"),
			Memory:           new("5Gi"),
			EphemeralStorage: new("10Gi"),
		},
		DisableMetrics:                 new(false),
		GCSFuseMetadataPrefetchOnMount: new(true),
		SkipCSIBucketAccessCheck:       new(true),
	}

	expected := &rayv1ac.RayClusterSpecApplyConfiguration{
		HeadGroupSpec: &rayv1ac.HeadGroupSpecApplyConfiguration{
			Template: &corev1ac.PodTemplateSpecApplyConfiguration{
				ObjectMetaApplyConfiguration: &metav1.ObjectMetaApplyConfiguration{
					Annotations: map[string]string{
						"gke-gcsfuse/cpu-request":               "1",
						"gke-gcsfuse/ephemeral-storage-limit":   "10Gi",
						"gke-gcsfuse/ephemeral-storage-request": "10Gi",
						"gke-gcsfuse/memory-limit":              "5Gi",
						"gke-gcsfuse/memory-request":            "5Gi",
					},
				},
				Spec: &corev1ac.PodSpecApplyConfiguration{
					Containers: []corev1ac.ContainerApplyConfiguration{
						{
							Name: new("ray-head"),
							VolumeMounts: []corev1ac.VolumeMountApplyConfiguration{
								{
									Name:      new("cluster-storage"),
									MountPath: new("/mnt/my-data"),
								},
							},
						},
					},
					Volumes: []corev1ac.VolumeApplyConfiguration{
						{
							Name: new("cluster-storage"),
							VolumeSourceApplyConfiguration: corev1ac.VolumeSourceApplyConfiguration{
								CSI: &corev1ac.CSIVolumeSourceApplyConfiguration{
									Driver: new(gcsFuseCSIDriver),
									VolumeAttributes: map[string]string{
										"bucketName":                     "my-bucket",
										"mountOptions":                   "uid=1234,gid=5678",
										"disableMetrics":                 "false",
										"gcsfuseMetadataPrefetchOnMount": "true",
										"skipCSIBucketAccessCheck":       "true",
									},
								},
							},
						},
					},
				},
			},
		},
		WorkerGroupSpecs: []rayv1ac.WorkerGroupSpecApplyConfiguration{
			{
				Template: &corev1ac.PodTemplateSpecApplyConfiguration{
					ObjectMetaApplyConfiguration: &metav1.ObjectMetaApplyConfiguration{
						Annotations: map[string]string{
							"gke-gcsfuse/cpu-request":               "1",
							"gke-gcsfuse/ephemeral-storage-limit":   "10Gi",
							"gke-gcsfuse/ephemeral-storage-request": "10Gi",
							"gke-gcsfuse/memory-limit":              "5Gi",
							"gke-gcsfuse/memory-request":            "5Gi",
						},
					},
					Spec: &corev1ac.PodSpecApplyConfiguration{
						Containers: []corev1ac.ContainerApplyConfiguration{
							{
								Name: new("ray-worker"),
								VolumeMounts: []corev1ac.VolumeMountApplyConfiguration{
									{
										Name:      new("cluster-storage"),
										MountPath: new("/mnt/my-data"),
									},
								},
							},
						},
						Volumes: []corev1ac.VolumeApplyConfiguration{
							{
								Name: new("cluster-storage"),
								VolumeSourceApplyConfiguration: corev1ac.VolumeSourceApplyConfiguration{
									CSI: &corev1ac.CSIVolumeSourceApplyConfiguration{
										Driver: new(gcsFuseCSIDriver),
										VolumeAttributes: map[string]string{
											"bucketName":                     "my-bucket",
											"mountOptions":                   "uid=1234,gid=5678",
											"disableMetrics":                 "false",
											"gcsfuseMetadataPrefetchOnMount": "true",
											"skipCSIBucketAccessCheck":       "true",
										},
									},
								},
							},
						},
					},
				},
			},
			{
				Template: &corev1ac.PodTemplateSpecApplyConfiguration{
					ObjectMetaApplyConfiguration: &metav1.ObjectMetaApplyConfiguration{
						Annotations: map[string]string{
							"gke-gcsfuse/cpu-request":               "1",
							"gke-gcsfuse/ephemeral-storage-limit":   "10Gi",
							"gke-gcsfuse/ephemeral-storage-request": "10Gi",
							"gke-gcsfuse/memory-limit":              "5Gi",
							"gke-gcsfuse/memory-request":            "5Gi",
						},
					},
					Spec: &corev1ac.PodSpecApplyConfiguration{
						Containers: []corev1ac.ContainerApplyConfiguration{
							{
								Name: new("ray-worker"),
								VolumeMounts: []corev1ac.VolumeMountApplyConfiguration{
									{
										Name:      new("cluster-storage"),
										MountPath: new("/mnt/my-data"),
									},
								},
							},
						},
						Volumes: []corev1ac.VolumeApplyConfiguration{
							{
								Name: new("cluster-storage"),
								VolumeSourceApplyConfiguration: corev1ac.VolumeSourceApplyConfiguration{
									CSI: &corev1ac.CSIVolumeSourceApplyConfiguration{
										Driver: new(gcsFuseCSIDriver),
										VolumeAttributes: map[string]string{
											"bucketName":                     "my-bucket",
											"mountOptions":                   "uid=1234,gid=5678",
											"disableMetrics":                 "false",
											"gcsfuseMetadataPrefetchOnMount": "true",
											"skipCSIBucketAccessCheck":       "true",
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	result := &rayv1ac.RayClusterSpecApplyConfiguration{
		HeadGroupSpec: &rayv1ac.HeadGroupSpecApplyConfiguration{
			Template: &corev1ac.PodTemplateSpecApplyConfiguration{
				Spec: &corev1ac.PodSpecApplyConfiguration{
					Containers: []corev1ac.ContainerApplyConfiguration{
						{
							Name: new("ray-head"),
						},
					},
				},
			},
		},
		WorkerGroupSpecs: []rayv1ac.WorkerGroupSpecApplyConfiguration{
			{
				Template: &corev1ac.PodTemplateSpecApplyConfiguration{
					Spec: &corev1ac.PodSpecApplyConfiguration{
						Containers: []corev1ac.ContainerApplyConfiguration{
							{
								Name: new("ray-worker"),
							},
						},
					},
				},
			},
			{
				Template: &corev1ac.PodTemplateSpecApplyConfiguration{
					Spec: &corev1ac.PodSpecApplyConfiguration{
						Containers: []corev1ac.ContainerApplyConfiguration{
							{
								Name: new("ray-worker"),
							},
						},
					},
				},
			},
		},
	}

	setGCSFuseOptions(result, gcsFuse)

	assert.Equal(t, expected, result)
}

func TestNewRayClusterConfigWithDefaults(t *testing.T) {
	result := newRayClusterConfigWithDefaults()
	expected := &RayClusterConfig{
		Image:      new(fmt.Sprintf("rayproject/ray:%s", util.RayVersion)),
		RayVersion: ptr.To(util.RayVersion),
		Head: &Head{
			CPU:    new("2"),
			Memory: new("4Gi"),
		},
		WorkerGroups: []WorkerGroup{
			{
				Name:     new("default-group"),
				CPU:      new("2"),
				Memory:   new("4Gi"),
				Replicas: int32(1),
			},
		},
	}

	assert.Equal(t, expected, result)
}

func TestParseConfigFile(t *testing.T) {
	tests := map[string]struct {
		config      string
		expected    *RayClusterConfig
		expectedErr string
	}{
		"invalid config": {
			config: `foo: bar`,
			expected: &RayClusterConfig{
				RayVersion: ptr.To(util.RayVersion),
				Image:      new(fmt.Sprintf("rayproject/ray:%s", util.RayVersion)),
			},
			expectedErr: "field foo not found in type generation.RayClusterConfig",
		},
		"empty config": {
			config: "",
			expected: &RayClusterConfig{
				RayVersion: ptr.To(util.RayVersion),
				Image:      new(fmt.Sprintf("rayproject/ray:%s", util.RayVersion)),
				Head: &Head{
					CPU:    new("2"),
					Memory: new("4Gi"),
				},
				WorkerGroups: []WorkerGroup{
					{
						Name:     new("default-group"),
						Replicas: int32(1),
						CPU:      new("2"),
						Memory:   new("4Gi"),
					},
				},
			},
		},
		"minimal config": {
			config: `worker-groups:
- replicas: 1
  gpu: 1
`,
			expected: &RayClusterConfig{
				RayVersion: ptr.To(util.RayVersion),
				Image:      new(fmt.Sprintf("rayproject/ray:%s", util.RayVersion)),
				Head: &Head{
					CPU:    new("2"),
					Memory: new("4Gi"),
				},
				WorkerGroups: []WorkerGroup{
					{
						Replicas: int32(1),
						GPU:      new("1"),
					},
				},
			},
		},
		"full config": {
			config: `namespace: hyperkube
name: dxia-test

labels:
  foo: bar
annotations:
  dead: beef

ray-version: 2.52.0
image: rayproject/ray:2.52.0

head:
  cpu: 3
  memory: 5Gi
  gpu: 0
  ephemeral-storage: 8Gi
  ray-start-params:
    metrics-export-port: 8082

worker-groups:
- name: cpu-workers
  replicas: 1
  cpu: 2
  memory: 4Gi
  gpu: 0
  ephemeral-storage: 12Gi
  ray-start-params:
    metrics-export-port: 8081
- name: gpu-workers
  replicas: 1
  cpu: 3
  memory: 6Gi
  gpu: 1
  ephemeral-storage: 13Gi
  ray-start-params:
    metrics-export-port: 8081

gke:
  # Cloud Storage FUSE options
  gcsfuse:
    bucket-name: my-bucket
    mount-path: /mnt/cluster_storage
    mount-options: "implicit-dirs,uid=1000,gid=100"
    resources:
      cpu: 250m
      memory: 256Mi
      ephemeral-storage: 5Gi
    disable-metrics: true
    gcsfuse-metadata-prefetch-on-mount: false
    skip-csi-bucket-access-check: false
`,
			expected: &RayClusterConfig{
				Namespace:   new("hyperkube"),
				Name:        new("dxia-test"),
				Labels:      map[string]string{"foo": "bar"},
				Annotations: map[string]string{"dead": "beef"},
				RayVersion:  new("2.52.0"),
				Image:       new("rayproject/ray:2.52.0"),
				Head: &Head{
					CPU:              new("3"),
					Memory:           new("5Gi"),
					GPU:              new("0"),
					RayStartParams:   map[string]string{"metrics-export-port": "8082"},
					EphemeralStorage: new("8Gi"),
				},
				WorkerGroups: []WorkerGroup{
					{
						Name:             new("cpu-workers"),
						Replicas:         int32(1),
						CPU:              new("2"),
						Memory:           new("4Gi"),
						GPU:              new("0"),
						EphemeralStorage: new("12Gi"),
						RayStartParams:   map[string]string{"metrics-export-port": "8081"},
					},
					{
						Name:             new("gpu-workers"),
						Replicas:         int32(1),
						CPU:              new("3"),
						Memory:           new("6Gi"),
						GPU:              new("1"),
						EphemeralStorage: new("13Gi"),
						RayStartParams:   map[string]string{"metrics-export-port": "8081"},
					},
				},
				GKE: &GKE{
					GCSFuse: &GCSFuse{
						BucketName:   "my-bucket",
						MountPath:    "/mnt/cluster_storage",
						MountOptions: new("implicit-dirs,uid=1000,gid=100"),
						Resources: &GCSFuseResources{
							CPU:              new("250m"),
							Memory:           new("256Mi"),
							EphemeralStorage: new("5Gi"),
						},
						DisableMetrics:                 new(true),
						GCSFuseMetadataPrefetchOnMount: new(false),
						SkipCSIBucketAccessCheck:       new(false),
					},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp("", "test-config.yaml")
			require.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			_, err = tmpFile.WriteString(test.config)
			require.NoError(t, err)

			result, err := ParseConfigFile(tmpFile.Name())
			if test.expectedErr != "" {
				assert.Contains(t, err.Error(), test.expectedErr)
				return
			}
			require.NoError(t, err)

			assert.Equal(t, test.expected, result)
		})
	}
}

func TestValidateConfig(t *testing.T) {
	tests := map[string]struct {
		config      *RayClusterConfig
		expectedErr string
	}{
		"invalid config": {
			config: &RayClusterConfig{
				Head: &Head{
					CPU: new("invalid"),
				},
			},
			expectedErr: "cpu is not a valid resource quantity",
		},
		"valid config": {
			config: &RayClusterConfig{
				Head: &Head{
					CPU:              new("2"),
					Memory:           new("4Gi"),
					GPU:              new("0"),
					EphemeralStorage: new("8Gi"),
				},
				WorkerGroups: []WorkerGroup{
					{
						CPU:              new("2"),
						Memory:           new("4Gi"),
						GPU:              new("0"),
						EphemeralStorage: new("8Gi"),
					},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateConfig(test.config)
			if test.expectedErr != "" {
				assert.Contains(t, err.Error(), test.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateGCSFuse(t *testing.T) {
	tests := map[string]struct {
		config      *GCSFuse
		expectedErr string
	}{
		"bucket name is required": {
			config:      &GCSFuse{},
			expectedErr: ".gcsfuse.bucket-name is required",
		},
		"mount path is required": {
			config: &GCSFuse{
				BucketName: "my-bucket",
			},
			expectedErr: ".gcsfuse.mount-path is required",
		},
		"valid config": {
			config: &GCSFuse{
				BucketName: "my-bucket",
				MountPath:  "/mnt/cluster_storage",
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateGCSFuse(test.config)
			if test.expectedErr != "" {
				assert.Contains(t, err.Error(), test.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestGetGCSFuseVolumeAttributes(t *testing.T) {
	config := &GCSFuse{
		BucketName:                     "my-bucket",
		MountPath:                      "/mnt/cluster_storage",
		MountOptions:                   new("implicit-dirs,uid=1000,gid=100"),
		DisableMetrics:                 new(true),
		GCSFuseMetadataPrefetchOnMount: new(false),
		SkipCSIBucketAccessCheck:       new(false),
	}

	expected := map[string]string{
		"bucketName":                     "my-bucket",
		"mountOptions":                   "implicit-dirs,uid=1000,gid=100",
		"disableMetrics":                 "true",
		"gcsfuseMetadataPrefetchOnMount": "false",
		"skipCSIBucketAccessCheck":       "false",
	}

	result := getGCSFuseVolumeAttributes(config)
	assert.Equal(t, expected, result)
}
