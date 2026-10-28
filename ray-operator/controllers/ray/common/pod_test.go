package common

import (
	"context"
	"fmt"
	"maps"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

var testMemoryLimit = resource.MustParse("1Gi")

var instance = rayv1.RayCluster{
	ObjectMeta: metav1.ObjectMeta{
		Name:      "raycluster-sample",
		Namespace: "default",
	},
	Spec: rayv1.RayClusterSpec{
		HeadGroupSpec: rayv1.HeadGroupSpec{
			RayStartParams: map[string]string{},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Namespace: "default",
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "ray-head",
							Image: "repo/image:custom",
							Env: []corev1.EnvVar{
								{
									Name:  "TEST_ENV_NAME",
									Value: "TEST_ENV_VALUE",
								},
							},
							Resources: corev1.ResourceRequirements{
								Requests: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: testMemoryLimit,
								},
								Limits: corev1.ResourceList{
									corev1.ResourceCPU:    resource.MustParse("1"),
									corev1.ResourceMemory: testMemoryLimit,
								},
							},
						},
					},
				},
			},
		},
		WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
			{
				Replicas:    ptr.To[int32](3),
				MinReplicas: ptr.To[int32](0),
				MaxReplicas: ptr.To[int32](10000),
				GroupName:   "small-group",
				RayStartParams: map[string]string{
					"port": "6379",
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "ray-worker",
								Image: "repo/image:custom",
								Resources: corev1.ResourceRequirements{
									Limits: corev1.ResourceList{
										corev1.ResourceCPU:    resource.MustParse("1"),
										corev1.ResourceMemory: testMemoryLimit,
										"nvidia.com/gpu":      resource.MustParse("3"),
									},
								},
								Env: []corev1.EnvVar{
									{
										Name:  "TEST_ENV_NAME",
										Value: "TEST_ENV_VALUE",
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

var volumesNoAutoscaler = []corev1.Volume{
	{
		Name: "shared-mem",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    corev1.StorageMediumMemory,
				SizeLimit: &testMemoryLimit,
			},
		},
	},
}

var volumesWithAutoscaler = []corev1.Volume{
	{
		Name: "shared-mem",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium:    corev1.StorageMediumMemory,
				SizeLimit: &testMemoryLimit,
			},
		},
	},
	{
		Name: "ray-logs",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				Medium: corev1.StorageMediumDefault,
			},
		},
	},
}

var volumeMountsNoAutoscaler = []corev1.VolumeMount{
	{
		Name:      "shared-mem",
		MountPath: "/dev/shm",
		ReadOnly:  false,
	},
}

var volumeMountsWithAutoscaler = []corev1.VolumeMount{
	{
		Name:      "shared-mem",
		MountPath: "/dev/shm",
		ReadOnly:  false,
	},
	{
		Name:      "ray-logs",
		MountPath: "/tmp/ray",
		ReadOnly:  false,
	},
}

var autoscalerContainer = corev1.Container{
	Name:            "autoscaler",
	Image:           "repo/image:custom",
	ImagePullPolicy: corev1.PullIfNotPresent,
	Env: []corev1.EnvVar{
		{
			Name: utils.RAY_CLUSTER_NAME,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: fmt.Sprintf("metadata.labels['%s']", utils.RayClusterLabelKey),
				},
			},
		},
		{
			Name: utils.RAY_CLUSTER_NAMESPACE,
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.namespace",
				},
			},
		},
		{
			Name: "RAY_HEAD_POD_NAME",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{
					FieldPath: "metadata.name",
				},
			},
		},
		{
			Name:  "KUBERAY_CRD_VER",
			Value: "v1",
		},
		{
			Name:  utils.KUBERAY_GEN_AUTOSCALER_START_CMD,
			Value: "ray kuberay-autoscaler --cluster-name $(RAY_CLUSTER_NAME) --cluster-namespace $(RAY_CLUSTER_NAMESPACE)",
		},
	},
	Command: []string{
		"/bin/bash",
		"-c",
		"--",
	},
	Args: []string{
		"ray kuberay-autoscaler --cluster-name $(RAY_CLUSTER_NAME) --cluster-namespace $(RAY_CLUSTER_NAMESPACE)",
	},
	Resources: corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	},
	VolumeMounts: []corev1.VolumeMount{
		{
			MountPath: "/tmp/ray",
			Name:      "ray-logs",
		},
	},
}

var trueFlag = true

func TestAddEmptyDirVolumes(t *testing.T) {
	testPod := &corev1.Pod{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name: "ray-worker",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "shared-mem",
							MountPath: "/dev/shm",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "shared-mem",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{
							Medium: corev1.StorageMediumMemory,
						},
					},
				},
			},
		},
	}
	assert.Len(t, testPod.Spec.Containers[0].VolumeMounts, 1)
	assert.Len(t, testPod.Spec.Volumes, 1)
	addEmptyDir(context.Background(), &testPod.Spec.Containers[0], testPod, "shared-mem2", "/dev/shm2", corev1.StorageMediumDefault)
	assert.Len(t, testPod.Spec.Containers[0].VolumeMounts, 2)
	assert.Len(t, testPod.Spec.Volumes, 2)
	addEmptyDir(context.Background(), &testPod.Spec.Containers[0], testPod, "shared-mem2", "/dev/shm2", corev1.StorageMediumDefault)
	assert.Len(t, testPod.Spec.Containers[0].VolumeMounts, 2)
	assert.Len(t, testPod.Spec.Volumes, 2)
}

func TestGetHeadPort(t *testing.T) {
	headStartParams := make(map[string]string)
	actualResult := GetHeadPort(headStartParams)
	assert.Equal(t, "6379", actualResult)

	headStartParams["port"] = "9999"
	actualResult = GetHeadPort(headStartParams)
	assert.Equal(t, "9999", actualResult)
}

func getEnvVar(container corev1.Container, envName string) *corev1.EnvVar {
	for _, env := range container.Env {
		if env.Name == envName {
			return &env
		}
	}
	return nil
}

func checkContainerEnv(t *testing.T, container corev1.Container, envName string, expectedValue string) {
	env := getEnvVar(container, envName)
	require.NotNil(t, env)

	if env.Value != "" {
		assert.Equal(t, expectedValue, env.Value)
	} else {
		// env.ValueFrom is the source for the environment variable's value. Cannot be used if value is not empty.
		// See https://pkg.go.dev/k8s.io/api/core/v1#EnvVar for more details.
		assert.Equal(t, expectedValue, env.ValueFrom.FieldRef.FieldPath)
	}
}

func assertWorkerGCSFaultToleranceConfig(t *testing.T, podTemplate *corev1.PodTemplateSpec, container corev1.Container) {
	assert.Empty(t, podTemplate.Annotations[utils.RayExternalStorageNSAnnotationKey])
	assert.Empty(t, podTemplate.Annotations[utils.RayFTEnabledAnnotationKey])
	assert.True(t, utils.EnvVarExists(utils.RAY_GCS_RPC_SERVER_RECONNECT_TIMEOUT_S, container.Env))
	assert.False(t, utils.EnvVarExists(utils.RAY_EXTERNAL_STORAGE_NS, container.Env))
	assert.False(t, utils.EnvVarExists(utils.RAY_REDIS_ADDRESS, container.Env))
	assert.False(t, utils.EnvVarExists(utils.REDIS_PASSWORD, container.Env))
}

func TestConfigureGCSFaultToleranceWithAnnotations(t *testing.T) {
	tests := []struct {
		name                        string
		storageNS                   string
		redisUsernameEnv            string
		redisPasswordEnv            string
		redisUsernameRayStartParams string
		redisPasswordRayStartParams string
		isHeadPod                   bool
		gcsFTEnabled                bool
	}{
		{
			name:         "GCS FT enabled",
			gcsFTEnabled: true,
			isHeadPod:    true,
		},
		{
			name:         "GCS FT enabled with external storage",
			gcsFTEnabled: true,
			storageNS:    "test-ns",
			isHeadPod:    true,
		},
		{
			name:             "GCS FT enabled with redis password env",
			gcsFTEnabled:     true,
			redisPasswordEnv: "test-password",
			isHeadPod:        true,
		},
		{
			name:             "GCS FT enabled with redis username and password env",
			gcsFTEnabled:     true,
			redisUsernameEnv: "test-username",
			redisPasswordEnv: "test-password",
			isHeadPod:        true,
		},
		{
			name:                        "GCS FT enabled with redis password ray start params",
			gcsFTEnabled:                true,
			redisPasswordRayStartParams: "test-password",
			isHeadPod:                   true,
		},
		{
			name:                        "GCS FT enabled with redis username and password ray start params",
			gcsFTEnabled:                true,
			redisUsernameRayStartParams: "test-username",
			redisPasswordRayStartParams: "test-password",
			isHeadPod:                   true,
		},
		{
			// The most common case.
			name:                        "GCS FT enabled with redis password env and ray start params referring to env",
			gcsFTEnabled:                true,
			redisPasswordEnv:            "test-password",
			redisPasswordRayStartParams: "$REDIS_PASSWORD",
			isHeadPod:                   false,
		},
		{
			name:                        "GCS FT enabled with redis username and password env and ray start params referring to env",
			gcsFTEnabled:                true,
			redisUsernameEnv:            "test-username",
			redisUsernameRayStartParams: "$REDIS_USERNAME",
			redisPasswordEnv:            "test-password",
			redisPasswordRayStartParams: "$REDIS_PASSWORD",
			isHeadPod:                   false,
		},
		{
			name:         "GCS FT enabled / worker Pod",
			gcsFTEnabled: true,
			isHeadPod:    false,
		},
		{
			name:         "GCS FT disabled",
			gcsFTEnabled: false,
			isHeadPod:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Validate the test input
			if test.redisUsernameEnv != "" && test.redisUsernameRayStartParams != "" {
				assert.Equal(t, "$REDIS_USERNAME", test.redisUsernameRayStartParams)
			}
			if test.redisPasswordEnv != "" && test.redisPasswordRayStartParams != "" {
				assert.Equal(t, "$REDIS_PASSWORD", test.redisPasswordRayStartParams)
			}

			// Prepare the cluster
			cluster := rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						utils.RayFTEnabledAnnotationKey: strconv.FormatBool(test.gcsFTEnabled),
					},
				},
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: map[string]string{},
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{
										Env: []corev1.EnvVar{
											{
												Name:  utils.RAY_REDIS_ADDRESS,
												Value: "redis:6379",
											},
										},
									},
								},
							},
						},
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
						{
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Env: []corev1.EnvVar{},
										},
									},
								},
							},
						},
					},
				},
			}
			if test.storageNS != "" {
				cluster.Annotations[utils.RayExternalStorageNSAnnotationKey] = test.storageNS
			}
			if test.redisUsernameEnv != "" {
				cluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Env = append(cluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Env, corev1.EnvVar{
					Name:  utils.REDIS_USERNAME,
					Value: test.redisUsernameEnv,
				})
			}
			if test.redisPasswordEnv != "" {
				cluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Env = append(cluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Env, corev1.EnvVar{
					Name:  utils.REDIS_PASSWORD,
					Value: test.redisPasswordEnv,
				})
			}
			if test.redisUsernameRayStartParams != "" {
				cluster.Spec.HeadGroupSpec.RayStartParams["redis-username"] = test.redisUsernameRayStartParams
			}
			if test.redisPasswordRayStartParams != "" {
				cluster.Spec.HeadGroupSpec.RayStartParams["redis-password"] = test.redisPasswordRayStartParams
			}
			podTemplate := &cluster.Spec.HeadGroupSpec.Template
			if !test.isHeadPod {
				podTemplate = &cluster.Spec.WorkerGroupSpecs[0].Template
			}

			// Configure GCS fault tolerance
			if test.isHeadPod {
				configureGCSFaultTolerance(podTemplate, cluster, rayv1.HeadNode)
			} else {
				configureGCSFaultTolerance(podTemplate, cluster, rayv1.WorkerNode)
			}

			// Check configurations for GCS fault tolerance
			container := podTemplate.Spec.Containers[utils.RayContainerIndex]
			if test.isHeadPod {
				assert.False(t, utils.EnvVarExists(utils.RAY_GCS_RPC_SERVER_RECONNECT_TIMEOUT_S, container.Env))
				assert.Equal(t, podTemplate.Annotations[utils.RayFTEnabledAnnotationKey], strconv.FormatBool(test.gcsFTEnabled))
				if test.storageNS != "" {
					assert.Equal(t, podTemplate.Annotations[utils.RayExternalStorageNSAnnotationKey], test.storageNS)
					assert.True(t, utils.EnvVarExists(utils.RAY_EXTERNAL_STORAGE_NS, container.Env))
				}
				if test.redisUsernameEnv != "" {
					env := getEnvVar(container, utils.REDIS_USERNAME)
					assert.Equal(t, env.Value, test.redisUsernameEnv)
				}
				if test.redisPasswordEnv != "" {
					env := getEnvVar(container, utils.REDIS_PASSWORD)
					assert.Equal(t, env.Value, test.redisPasswordEnv)
				} else if test.redisPasswordRayStartParams != "" {
					env := getEnvVar(container, utils.REDIS_PASSWORD)
					assert.Equal(t, env.Value, test.redisPasswordRayStartParams)
				}
			} else {
				assertWorkerGCSFaultToleranceConfig(t, podTemplate, container)
			}
		})
	}
}

func TestConfigureGCSFaultToleranceWithGcsFTOptions(t *testing.T) {
	tests := []struct {
		gcsFTOptions *rayv1.GcsFaultToleranceOptions
		name         string
		isHeadPod    bool
	}{
		{
			name: "GCS FT enabled",
			gcsFTOptions: &rayv1.GcsFaultToleranceOptions{
				RedisAddress: "redis:6379",
			},
			isHeadPod: true,
		},
		{
			name: "GCS FT enabled with redis password",
			gcsFTOptions: &rayv1.GcsFaultToleranceOptions{
				RedisAddress: "redis:6379",
				RedisPassword: &rayv1.RedisCredential{
					Value: "test-password",
				},
			},
			isHeadPod: true,
		},
		{
			name: "GCS FT enabled with redis username and password",
			gcsFTOptions: &rayv1.GcsFaultToleranceOptions{
				RedisAddress: "redis:6379",
				RedisUsername: &rayv1.RedisCredential{
					Value: "test-username",
				},
				RedisPassword: &rayv1.RedisCredential{
					Value: "test-password",
				},
			},
			isHeadPod: true,
		},
		{
			name: "GCS FT enabled with redis password in secret",
			gcsFTOptions: &rayv1.GcsFaultToleranceOptions{
				RedisAddress: "redis:6379",
				RedisPassword: &rayv1.RedisCredential{
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "spec.redisPassword",
						},
					},
				},
			},
			isHeadPod: true,
		},
		{
			name: "GCS FT enabled with redis username and password in secret",
			gcsFTOptions: &rayv1.GcsFaultToleranceOptions{
				RedisAddress: "redis:6379",
				RedisUsername: &rayv1.RedisCredential{
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "spec.redisUsername",
						},
					},
				},
				RedisPassword: &rayv1.RedisCredential{
					ValueFrom: &corev1.EnvVarSource{
						FieldRef: &corev1.ObjectFieldSelector{
							FieldPath: "spec.redisPassword",
						},
					},
				},
			},
			isHeadPod: true,
		},
		{
			name: "GCS FT enabled with redis external storage namespace",
			gcsFTOptions: &rayv1.GcsFaultToleranceOptions{
				RedisAddress:             "redis:6379",
				ExternalStorageNamespace: "test-ns",
			},
			isHeadPod: true,
		},
		{
			name: "GCS FT enabled / worker Pod",
			gcsFTOptions: &rayv1.GcsFaultToleranceOptions{
				RedisAddress: "redis:6379",
			},
			isHeadPod: false,
		},
	}

	emptyPodTemplate := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Env: []corev1.EnvVar{},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Prepare the cluster
			cluster := rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					GcsFaultToleranceOptions: test.gcsFTOptions,
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: map[string]string{},
						Template:       *emptyPodTemplate.DeepCopy(),
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
						{
							Template: *emptyPodTemplate.DeepCopy(),
						},
					},
				},
			}

			podTemplate := &cluster.Spec.HeadGroupSpec.Template
			nodeType := rayv1.HeadNode
			if !test.isHeadPod {
				podTemplate = &cluster.Spec.WorkerGroupSpecs[0].Template
				nodeType = rayv1.WorkerNode
			}
			configureGCSFaultTolerance(podTemplate, cluster, nodeType)
			container := podTemplate.Spec.Containers[utils.RayContainerIndex]

			if test.isHeadPod {
				assert.False(t, utils.EnvVarExists(utils.RAY_GCS_RPC_SERVER_RECONNECT_TIMEOUT_S, container.Env))
				assert.Equal(t, podTemplate.Annotations[utils.RayFTEnabledAnnotationKey], strconv.FormatBool(test.gcsFTOptions != nil))

				env := getEnvVar(container, utils.RAY_REDIS_ADDRESS)
				assert.Equal(t, "redis:6379", env.Value)

				if test.gcsFTOptions.RedisUsername != nil {
					env := getEnvVar(container, utils.REDIS_USERNAME)
					assert.Equal(t, env.Value, test.gcsFTOptions.RedisUsername.Value)
					assert.Equal(t, env.ValueFrom, test.gcsFTOptions.RedisUsername.ValueFrom)
				}

				if test.gcsFTOptions.RedisPassword != nil {
					env := getEnvVar(container, utils.REDIS_PASSWORD)
					assert.Equal(t, env.Value, test.gcsFTOptions.RedisPassword.Value)
					assert.Equal(t, env.ValueFrom, test.gcsFTOptions.RedisPassword.ValueFrom)
				}
				if test.gcsFTOptions.ExternalStorageNamespace != "" {
					assert.Equal(t, podTemplate.Annotations[utils.RayExternalStorageNSAnnotationKey], test.gcsFTOptions.ExternalStorageNamespace)
					env := getEnvVar(container, utils.RAY_EXTERNAL_STORAGE_NS)
					assert.Equal(t, env.Value, test.gcsFTOptions.ExternalStorageNamespace)
				}
			} else {
				assertWorkerGCSFaultToleranceConfig(t, podTemplate, container)
			}
		})
	}
}

func TestConfigureGCSFaultToleranceEmbedded(t *testing.T) {
	emptyPodTemplate := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{Env: []corev1.EnvVar{}},
			},
		},
	}

	tests := []struct {
		name          string
		options       *rayv1.GcsFaultToleranceOptions
		wantClaimName string
		wantSubPath   string
	}{
		{
			name:          "operator-managed PVC",
			options:       &rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRocksDB},
			wantClaimName: "test-cluster-gcs-pvc",
		},
		{
			name: "claimName resolves to user PVC",
			options: &rayv1.GcsFaultToleranceOptions{
				Backend: rayv1.GcsFTBackendRocksDB,
				Storage: &rayv1.GcsEmbeddedStorage{ClaimName: "my-pvc"},
			},
			wantClaimName: "my-pvc",
		},
		{
			name: "subPath is wired onto the mount",
			options: &rayv1.GcsFaultToleranceOptions{
				Backend: rayv1.GcsFTBackendRocksDB,
				Storage: &rayv1.GcsEmbeddedStorage{SubPath: "clusters/foo"},
			},
			wantClaimName: "test-cluster-gcs-pvc",
			wantSubPath:   "clusters/foo",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", UID: "test-uid"},
				Spec: rayv1.RayClusterSpec{
					GcsFaultToleranceOptions: test.options,
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: map[string]string{},
						Template:       *emptyPodTemplate.DeepCopy(),
					},
				},
			}

			podTemplate := &cluster.Spec.HeadGroupSpec.Template
			configureGCSFaultTolerance(podTemplate, cluster, rayv1.HeadNode)
			container := podTemplate.Spec.Containers[utils.RayContainerIndex]

			// The RocksDB backend env vars are set.
			assert.Equal(t, utils.GCSStorageRocksDBValue, getEnvVar(container, utils.RAY_GCS_STORAGE).Value)
			assert.Equal(t, utils.GCSStorageMountPath, getEnvVar(container, utils.RAY_GCS_STORAGE_PATH).Value)

			// No Redis env vars leak onto the embedded path.
			assert.False(t, utils.EnvVarExists(utils.RAY_REDIS_ADDRESS, container.Env))
			assert.False(t, utils.EnvVarExists(utils.RAY_EXTERNAL_STORAGE_NS, container.Env))

			// The volume + mount are wired to the resolved claim name.
			require.Len(t, container.VolumeMounts, 1)
			assert.Equal(t, utils.GCSStorageVolumeName, container.VolumeMounts[0].Name)
			assert.Equal(t, utils.GCSStorageMountPath, container.VolumeMounts[0].MountPath)
			assert.Equal(t, test.wantSubPath, container.VolumeMounts[0].SubPath)

			require.Len(t, podTemplate.Spec.Volumes, 1)
			require.NotNil(t, podTemplate.Spec.Volumes[0].PersistentVolumeClaim)
			assert.Equal(t, test.wantClaimName, podTemplate.Spec.Volumes[0].PersistentVolumeClaim.ClaimName)
		})
	}
}

func TestBuildPod(t *testing.T) {
	cluster := instance.DeepCopy()
	ctx := context.Background()

	// Define default environment variables
	defaultContainerEnvs := []corev1.EnvVar{
		{Name: "TEST_DEFAULT_ENV_NAME", Value: "TEST_ENV_VALUE"},
	}

	// Test head pod
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod := BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", false, utils.GetCRDType(""), "", defaultContainerEnvs, "")

	// Check environment variables
	rayContainer := pod.Spec.Containers[utils.RayContainerIndex]
	checkContainerEnv(t, rayContainer, utils.RAY_ADDRESS, "127.0.0.1:6379")
	checkContainerEnv(t, rayContainer, utils.RAY_USAGE_STATS_KUBERAY_IN_USE, "1")
	checkContainerEnv(t, rayContainer, utils.RAY_CLUSTER_NAME, fmt.Sprintf("metadata.labels['%s']", utils.RayClusterLabelKey))
	checkContainerEnv(t, rayContainer, utils.RAY_CLUSTER_NAMESPACE, "metadata.namespace")
	checkContainerEnv(t, rayContainer, utils.RAY_DASHBOARD_ENABLE_K8S_DISK_USAGE, "1")
	checkContainerEnv(t, rayContainer, utils.RAY_NODE_TYPE_NAME, fmt.Sprintf("metadata.labels['%s']", utils.RayNodeGroupLabelKey))
	checkContainerEnv(t, rayContainer, utils.RAY_USAGE_STATS_EXTRA_TAGS, fmt.Sprintf("kuberay_version=%s;kuberay_crd=%s", utils.KUBERAY_VERSION, utils.RayClusterCRD))
	headRayStartCommandEnv := getEnvVar(rayContainer, utils.KUBERAY_GEN_RAY_START_CMD)
	assert.Contains(t, headRayStartCommandEnv.Value, "ray start")

	// In head, init container needs FQ_RAY_IP to create a self-signed certificate for its TLS authenticate.
	for _, initContainer := range pod.Spec.InitContainers {
		checkContainerEnv(t, initContainer, utils.FQ_RAY_IP, "raycluster-sample-head-svc.default.svc.cluster.local")
		checkContainerEnv(t, rayContainer, utils.RAY_IP, "raycluster-sample-head-svc")
	}

	// Check labels.
	assert.Equal(t, cluster.Name, pod.Labels[utils.RayClusterLabelKey])
	assert.Equal(t, string(rayv1.HeadNode), pod.Labels[utils.RayNodeTypeLabelKey])
	assert.Equal(t, utils.RayNodeHeadGroupLabelValue, pod.Labels[utils.RayNodeGroupLabelKey])

	// Check volumes.
	actualVolumes := pod.Spec.Volumes
	expectedVolumes := volumesNoAutoscaler
	assert.True(t, reflect.DeepEqual(actualVolumes, expectedVolumes))

	// Check volume mounts.
	actualVolumeMounts := pod.Spec.Containers[0].VolumeMounts
	expectedVolumeMounts := volumeMountsNoAutoscaler
	assert.True(t, reflect.DeepEqual(actualVolumeMounts, expectedVolumeMounts))

	// testing worker pod
	worker := cluster.Spec.WorkerGroupSpecs[0]
	podName = cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	podTemplateSpec = DefaultWorkerPodTemplate(ctx, *cluster, worker, podName, fqdnRayIP, "6379", "", 0, 0)
	pod = BuildPod(ctx, podTemplateSpec, rayv1.WorkerNode, worker.RayStartParams, "6379", false, utils.GetCRDType(""), fqdnRayIP, defaultContainerEnvs, "")

	// Check resources
	rayContainer = pod.Spec.Containers[utils.RayContainerIndex]
	expectedResources := corev1.ResourceRequirements{
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: testMemoryLimit,
			"nvidia.com/gpu":      resource.MustParse("3"),
		},
	}
	assert.Equal(t, expectedResources.Limits, rayContainer.Resources.Limits, "Resource limits do not match")

	// Check environment variables
	rayContainer = pod.Spec.Containers[utils.RayContainerIndex]
	checkContainerEnv(t, rayContainer, utils.RAY_ADDRESS, "raycluster-sample-head-svc.default.svc.cluster.local:6379")
	checkContainerEnv(t, rayContainer, utils.FQ_RAY_IP, "raycluster-sample-head-svc.default.svc.cluster.local")
	checkContainerEnv(t, rayContainer, utils.RAY_IP, "raycluster-sample-head-svc")
	checkContainerEnv(t, rayContainer, utils.RAY_CLUSTER_NAME, fmt.Sprintf("metadata.labels['%s']", utils.RayClusterLabelKey))
	checkContainerEnv(t, rayContainer, utils.RAY_CLUSTER_NAMESPACE, "metadata.namespace")
	checkContainerEnv(t, rayContainer, utils.RAY_DASHBOARD_ENABLE_K8S_DISK_USAGE, "1")
	checkContainerEnv(t, rayContainer, utils.RAY_NODE_TYPE_NAME, fmt.Sprintf("metadata.labels['%s']", utils.RayNodeGroupLabelKey))
	workerRayStartCommandEnv := getEnvVar(rayContainer, utils.KUBERAY_GEN_RAY_START_CMD)
	assert.Contains(t, workerRayStartCommandEnv.Value, "ray start")

	expectedCommandArg := splitAndSort("ulimit -n ${RAY_START_ULIMIT_OPEN_FILES:-65536}; ray start --block --dashboard-agent-listen-port=52365 --memory=1073741824 --num-cpus=1 --num-gpus=3 --address=raycluster-sample-head-svc.default.svc.cluster.local:6379 --port=6379 --metrics-export-port=8080")
	actualCommandArg := splitAndSort(pod.Spec.Containers[0].Args[0])
	assert.Equal(t, expectedCommandArg, actualCommandArg)

	// Check Envs
	rayContainer = pod.Spec.Containers[utils.RayContainerIndex]
	checkContainerEnv(t, rayContainer, "TEST_ENV_NAME", "TEST_ENV_VALUE")

	// Test default environment variables injection in ray pods
	checkContainerEnv(t, rayContainer, "TEST_DEFAULT_ENV_NAME", "TEST_ENV_VALUE")
}

func TestBuildPod_WithUlimitOverride(t *testing.T) {
	cluster := instance.DeepCopy()
	ctx := context.Background()

	// Add RAY_START_ULIMIT_OPEN_FILES to container env
	cluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Env = append(
		cluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Env,
		corev1.EnvVar{Name: utils.RAY_START_ULIMIT_OPEN_FILES, Value: "1048576"},
	)

	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod := BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", false, utils.GetCRDType(""), "", nil, "")

	// The generated command arg still uses ${RAY_START_ULIMIT_OPEN_FILES:-65536} because the shell resolves it at runtime.
	expectedCommandArg := splitAndSort("ulimit -n ${RAY_START_ULIMIT_OPEN_FILES:-65536}; ray start --head --block --dashboard-agent-listen-port=52365 --memory=1073741824 --num-cpus=1 --metrics-export-port=8080 --dashboard-host=0.0.0.0")
	actualCommandArg := splitAndSort(pod.Spec.Containers[0].Args[0])
	assert.Equal(t, expectedCommandArg, actualCommandArg)

	// Verify that the environment variable exists in the container.
	rayContainer := pod.Spec.Containers[utils.RayContainerIndex]
	checkContainerEnv(t, rayContainer, utils.RAY_START_ULIMIT_OPEN_FILES, "1048576")
}

func TestBuildAutoscalerContainer(t *testing.T) {
	const autoscalerImage = "rayproject/ray:2.52.0"
	const expectedCmd = "ray kuberay-autoscaler --cluster-name $(RAY_CLUSTER_NAME) --cluster-namespace $(RAY_CLUSTER_NAMESPACE)"

	t.Run("KUBERAY_GEN_AUTOSCALER_START_CMD is always injected with the generated command", func(t *testing.T) {
		container := BuildAutoscalerContainer(autoscalerImage)

		env := getEnvVar(container, utils.KUBERAY_GEN_AUTOSCALER_START_CMD)
		require.NotNil(t, env, "KUBERAY_GEN_AUTOSCALER_START_CMD env var should always be present")
		assert.Equal(t, expectedCmd, env.Value)
	})

	t.Run("Default Args equal KUBERAY_GEN_AUTOSCALER_START_CMD value", func(t *testing.T) {
		container := BuildAutoscalerContainer(autoscalerImage)

		require.Len(t, container.Args, 1)
		assert.Equal(t, expectedCmd, container.Args[0],
			"Default Args should match KUBERAY_GEN_AUTOSCALER_START_CMD so they are consistent")
	})

	t.Run("AutoscalerOptions.Command override replaces command", func(t *testing.T) {
		container := BuildAutoscalerContainer(autoscalerImage)
		customCMD := []string{"/bin/bash", "-lc", "--"}
		mergeAutoscalerOverrides(&container, &rayv1.AutoscalerOptions{
			Command: customCMD,
		})

		// Args must reflect the override.
		assert.Equal(t, customCMD, container.Command)

		// KUBERAY_GEN_AUTOSCALER_START_CMD must still hold the original generated command.
		env := getEnvVar(container, utils.KUBERAY_GEN_AUTOSCALER_START_CMD)
		require.NotNil(t, env)
		assert.Equal(t, expectedCmd, env.Value)
	})

	t.Run("AutoscalerOptions.Args override replaces Args but not KUBERAY_GEN_AUTOSCALER_START_CMD", func(t *testing.T) {
		container := BuildAutoscalerContainer(autoscalerImage)
		customArgs := []string{"ulimit -n 65536; $KUBERAY_GEN_AUTOSCALER_START_CMD"}
		mergeAutoscalerOverrides(&container, &rayv1.AutoscalerOptions{
			Args: customArgs,
		})

		// Args must reflect the override.
		assert.Equal(t, customArgs, container.Args)

		// KUBERAY_GEN_AUTOSCALER_START_CMD must still hold the original generated command.
		env := getEnvVar(container, utils.KUBERAY_GEN_AUTOSCALER_START_CMD)
		require.NotNil(t, env)
		assert.Equal(t, expectedCmd, env.Value)
	})

	t.Run("AutoscalerOptions.Env additions do not overwrite KUBERAY_GEN_AUTOSCALER_START_CMD", func(t *testing.T) {
		container := BuildAutoscalerContainer(autoscalerImage)
		mergeAutoscalerOverrides(&container, &rayv1.AutoscalerOptions{
			Env: []corev1.EnvVar{
				{Name: "AUTOSCALER_UPDATE_INTERVAL_S", Value: "10"},
			},
		})

		// The user-supplied env var must be present.
		userEnv := getEnvVar(container, "AUTOSCALER_UPDATE_INTERVAL_S")
		require.NotNil(t, userEnv)
		assert.Equal(t, "10", userEnv.Value)

		// KUBERAY_GEN_AUTOSCALER_START_CMD must still reflect the KubeRay-generated command.
		env := getEnvVar(container, utils.KUBERAY_GEN_AUTOSCALER_START_CMD)
		require.NotNil(t, env)
		assert.Equal(t, expectedCmd, env.Value)
	})
}

func TestBuildPod_WithPlasmaDirectory(t *testing.T) {
	ctx := context.Background()

	testCases := []struct {
		rayStartParams     map[string]string
		name               string
		expectSharedMemory bool
	}{
		{
			name:               "unset plasma-directory keeps shared memory mount",
			rayStartParams:     map[string]string{},
			expectSharedMemory: true,
		},
		{
			name: "plasma-directory set to /dev/shm skips shared memory mount",
			rayStartParams: map[string]string{
				PlasmaDirectoryParamKey: "/dev/shm",
			},
			expectSharedMemory: false,
		},
		{
			name: "plasma-directory set to /dev/shm/ skips shared memory mount",
			rayStartParams: map[string]string{
				PlasmaDirectoryParamKey: "/dev/shm/",
			},
			expectSharedMemory: false,
		},
		{
			name: "non /dev/shm plasma-directory skips shared memory mount",
			rayStartParams: map[string]string{
				PlasmaDirectoryParamKey: "/tmp/ray/plasma",
			},
			expectSharedMemory: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cluster := instance.DeepCopy()
			podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
			podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
			pod := BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, maps.Clone(tc.rayStartParams), "6379", false, utils.GetCRDType(""), "", nil, "")

			assert.Equal(t, tc.expectSharedMemory, checkIfVolumeMounted(&pod.Spec.Containers[utils.RayContainerIndex], SharedMemoryVolumeMountPath))
			assert.Equal(t, tc.expectSharedMemory, checkIfVolumeExists(&pod, SharedMemoryVolumeName))
		})
	}
}

func TestBuildPod_WithEnableK8sTokenAuth(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	cluster.Spec.AuthOptions = &rayv1.AuthOptions{
		Mode:               rayv1.AuthModeToken,
		EnableK8sTokenAuth: new(true),
	}

	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod := BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", false, utils.GetCRDType(""), "", nil, "")

	rayContainer := pod.Spec.Containers[utils.RayContainerIndex]
	checkContainerEnv(t, rayContainer, utils.RAY_ENABLE_K8S_TOKEN_AUTH_ENV_VAR, "true")

	foundVolumeMount := false
	for _, vm := range rayContainer.VolumeMounts {
		if vm.Name == utils.RayTokenVolumeName && vm.MountPath == utils.RayTokenMountPath && vm.ReadOnly {
			foundVolumeMount = true
			break
		}
	}
	if !foundVolumeMount {
		t.Errorf("Ray container should have the ray-token volume mount")
	}

	foundVolume := false
	for _, v := range pod.Spec.Volumes {
		if v.Name == utils.RayTokenVolumeName && v.Projected != nil {
			foundVolume = true
			break
		}
	}
	if !foundVolume {
		t.Errorf("Pod should have the ray-token volume")
	}

	cluster = instance.DeepCopy()
	cluster.Spec.AuthOptions = &rayv1.AuthOptions{
		Mode:               rayv1.AuthModeToken,
		EnableK8sTokenAuth: new(false),
	}
	podTemplateSpec = DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod = BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", false, utils.GetCRDType(""), "", nil, "")
	rayContainer = pod.Spec.Containers[utils.RayContainerIndex]
	for _, env := range rayContainer.Env {
		if env.Name == utils.RAY_ENABLE_K8S_TOKEN_AUTH_ENV_VAR {
			t.Errorf("RAY_ENABLE_K8S_TOKEN_AUTH should not be set")
		}
	}

	cluster = instance.DeepCopy()
	cluster.Spec.AuthOptions = &rayv1.AuthOptions{
		Mode:               rayv1.AuthModeToken,
		EnableK8sTokenAuth: nil,
	}
	podTemplateSpec = DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod = BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", false, utils.GetCRDType(""), "", nil, "")
	rayContainer = pod.Spec.Containers[utils.RayContainerIndex]
	for _, env := range rayContainer.Env {
		if env.Name == utils.RAY_ENABLE_K8S_TOKEN_AUTH_ENV_VAR {
			t.Errorf("RAY_ENABLE_K8S_TOKEN_AUTH should not be set")
		}
	}
}

func TestBuildPod_WithEnableK8sTokenAuth_InitContainer(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	cluster.Spec.AuthOptions = &rayv1.AuthOptions{
		Mode:               rayv1.AuthModeToken,
		EnableK8sTokenAuth: new(true),
	}

	worker := cluster.Spec.WorkerGroupSpecs[0]
	podName := cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	podTemplateSpec := DefaultWorkerPodTemplate(ctx, *cluster, worker, podName, fqdnRayIP, "6379", "", 0, 0)
	pod := BuildPod(ctx, podTemplateSpec, rayv1.WorkerNode, worker.RayStartParams, "6379", false, utils.GetCRDType(""), fqdnRayIP, nil, "")

	foundInitContainer := false
	for _, container := range pod.Spec.InitContainers {
		if container.Name == "wait-gcs-ready" {
			foundInitContainer = true
			foundVolumeMount := false
			for _, vm := range container.VolumeMounts {
				if vm.Name == utils.RayTokenVolumeName && vm.MountPath == utils.RayTokenMountPath && vm.ReadOnly {
					foundVolumeMount = true
					break
				}
			}
			if !foundVolumeMount {
				t.Errorf("wait-gcs-ready init container should have the ray-token volume mount")
			}
		}
	}
	if !foundInitContainer {
		t.Errorf("wait-gcs-ready init container should be present")
	}
}

func TestBuildPod_WithNoCPULimits(t *testing.T) {
	cluster := instance.DeepCopy()
	ctx := context.Background()

	cluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: testMemoryLimit,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceMemory: testMemoryLimit,
		},
	}
	cluster.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[utils.RayContainerIndex].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: testMemoryLimit,
		},

		Limits: corev1.ResourceList{
			corev1.ResourceMemory: testMemoryLimit,
			"nvidia.com/gpu":      resource.MustParse("3"),
		},
	}

	// Test head pod
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod := BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", false, utils.GetCRDType(""), "", nil, "")
	expectedCommandArg := splitAndSort("ulimit -n ${RAY_START_ULIMIT_OPEN_FILES:-65536}; ray start --head --block --dashboard-agent-listen-port=52365 --memory=1073741824 --num-cpus=2 --metrics-export-port=8080 --dashboard-host=0.0.0.0")
	actualCommandArg := splitAndSort(pod.Spec.Containers[0].Args[0])
	assert.Equal(t, expectedCommandArg, actualCommandArg)

	// testing worker pod
	worker := cluster.Spec.WorkerGroupSpecs[0]
	podName = cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	podTemplateSpec = DefaultWorkerPodTemplate(ctx, *cluster, worker, podName, fqdnRayIP, "6379", "", 0, 0)
	pod = BuildPod(ctx, podTemplateSpec, rayv1.WorkerNode, worker.RayStartParams, "6379", false, utils.GetCRDType(""), fqdnRayIP, nil, "")
	expectedCommandArg = splitAndSort("ulimit -n ${RAY_START_ULIMIT_OPEN_FILES:-65536}; ray start --block --dashboard-agent-listen-port=52365 --memory=1073741824 --num-cpus=2 --num-gpus=3 --address=raycluster-sample-head-svc.default.svc.cluster.local:6379 --port=6379 --metrics-export-port=8080")
	actualCommandArg = splitAndSort(pod.Spec.Containers[0].Args[0])
	assert.Equal(t, expectedCommandArg, actualCommandArg)
}

func TestBuildPod_WithOverwriteCommand(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	cluster.Annotations = map[string]string{
		// When the value of the annotation is "true", KubeRay will not generate the command and args for the container.
		// Instead, it will use the command and args specified by the use.
		utils.RayOverwriteContainerCmdAnnotationKey: "true",
	}
	cluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Command = []string{"I am head"}
	cluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Args = []string{"I am head again"}
	cluster.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[utils.RayContainerIndex].Command = []string{"I am worker"}
	cluster.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[utils.RayContainerIndex].Args = []string{"I am worker again"}

	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	headPod := BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", false, utils.GetCRDType(""), "", nil, "")
	headContainer := headPod.Spec.Containers[utils.RayContainerIndex]
	assert.Equal(t, []string{"I am head"}, headContainer.Command)
	assert.Equal(t, []string{"I am head again"}, headContainer.Args)

	worker := cluster.Spec.WorkerGroupSpecs[0]
	podName = cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	podTemplateSpec = DefaultWorkerPodTemplate(ctx, *cluster, worker, podName, fqdnRayIP, "6379", "", 0, 0)
	workerPod := BuildPod(ctx, podTemplateSpec, rayv1.WorkerNode, worker.RayStartParams, "6379", false, utils.GetCRDType(""), fqdnRayIP, nil, "")
	workerContainer := workerPod.Spec.Containers[utils.RayContainerIndex]
	assert.Equal(t, []string{"I am worker"}, workerContainer.Command)
	assert.Equal(t, []string{"I am worker again"}, workerContainer.Args)
}

func TestBuildPod_WithAutoscalerEnabled(t *testing.T) {
	ctx := context.Background()
	cluster := instance.DeepCopy()
	cluster.Spec.EnableInTreeAutoscaling = &trueFlag
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod := BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", true, utils.GetCRDType(""), "", nil, "")

	assert.Equal(t, cluster.Name, pod.Labels[utils.RayClusterLabelKey])
	assert.Equal(t, string(rayv1.HeadNode), pod.Labels[utils.RayNodeTypeLabelKey])
	assert.Equal(t, utils.RayNodeHeadGroupLabelValue, pod.Labels[utils.RayNodeGroupLabelKey])

	// verify no-monitoring is set. conversion only happens in BuildPod so we can only check it here.
	assert.Contains(t, pod.Spec.Containers[0].Args[0], "--no-monitor")

	actualVolumes := pod.Spec.Volumes
	expectedVolumes := volumesWithAutoscaler
	assert.True(t, reflect.DeepEqual(actualVolumes, expectedVolumes))

	actualVolumeMounts := pod.Spec.Containers[0].VolumeMounts
	expectedVolumeMounts := volumeMountsWithAutoscaler
	assert.True(t, reflect.DeepEqual(actualVolumeMounts, expectedVolumeMounts))

	// Make sure autoscaler container was formatted correctly.
	assert.Len(t, pod.Spec.Containers, 2)
	index := getAutoscalerContainerIndex(pod)
	actualContainer := pod.Spec.Containers[index]
	expectedContainer := autoscalerContainer
	assert.True(t, reflect.DeepEqual(expectedContainer, actualContainer))
}

func TestBuildPod_WithCreatedByRayService(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	cluster.Spec.EnableInTreeAutoscaling = &trueFlag
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod := BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", true, utils.RayServiceCRD, "", nil, "")

	val, ok := pod.Labels[utils.RayClusterServingServiceLabelKey]
	assert.True(t, ok, "Expected serve label is not present")
	assert.Equal(t, utils.EnableRayClusterServingServiceFalse, val, "Wrong serve label value")
	utils.EnvVarExists(utils.RAY_TIMEOUT_MS_TASK_WAIT_FOR_DEATH_INFO, pod.Spec.Containers[utils.RayContainerIndex].Env)

	worker := cluster.Spec.WorkerGroupSpecs[0]
	podName = cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	podTemplateSpec = DefaultWorkerPodTemplate(ctx, *cluster, worker, podName, fqdnRayIP, "6379", "", 0, 0)
	pod = BuildPod(ctx, podTemplateSpec, rayv1.WorkerNode, worker.RayStartParams, "6379", false, utils.RayServiceCRD, fqdnRayIP, nil, "")

	val, ok = pod.Labels[utils.RayClusterServingServiceLabelKey]
	assert.True(t, ok, "Expected serve label is not present")
	assert.Equal(t, utils.EnableRayClusterServingServiceTrue, val, "Wrong serve label value")
	utils.EnvVarExists(utils.RAY_TIMEOUT_MS_TASK_WAIT_FOR_DEATH_INFO, pod.Spec.Containers[utils.RayContainerIndex].Env)
}

func TestBuildPod_WithLoginBash(t *testing.T) {
	tests := []struct {
		name            string
		expectedCmd     []string
		enableLoginBash bool
	}{
		{
			name:            "With login bash enabled",
			enableLoginBash: true,
			expectedCmd:     []string{"/bin/bash", "-cl", "--"},
		},
		{
			name:            "Without login bash enabled",
			enableLoginBash: false,
			expectedCmd:     []string{"/bin/bash", "-c", "--"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if tc.enableLoginBash {
				os.Setenv(utils.ENABLE_LOGIN_SHELL, "true")
				defer os.Unsetenv(utils.ENABLE_LOGIN_SHELL)
			}

			cluster := instance.DeepCopy()
			cluster.Spec.EnableInTreeAutoscaling = &trueFlag

			// Test head pod
			podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
			podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
			headPod := BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", true, utils.RayServiceCRD, "", nil, "")

			// Verify head container command
			headContainer := headPod.Spec.Containers[utils.RayContainerIndex]
			assert.Equal(t, tc.expectedCmd, headContainer.Command)

			// Verify autoscaler container command
			index := getAutoscalerContainerIndex(headPod)
			autoscalerContainer := headPod.Spec.Containers[index]
			assert.Equal(t, tc.expectedCmd, autoscalerContainer.Command)

			// Test worker pod
			worker := cluster.Spec.WorkerGroupSpecs[0]
			podName = cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
			fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
			podTemplateSpec = DefaultWorkerPodTemplate(ctx, *cluster, worker, podName, fqdnRayIP, "6379", "", 0, 0)
			workerPod := BuildPod(ctx, podTemplateSpec, rayv1.WorkerNode, worker.RayStartParams, "6379", false, utils.RayServiceCRD, fqdnRayIP, nil, "")

			// Verify worker container command
			workerContainer := workerPod.Spec.Containers[utils.RayContainerIndex]
			assert.Equal(t, tc.expectedCmd, workerContainer.Command)

			// Verify init container command
			initContainers := workerPod.Spec.InitContainers
			for _, initContainer := range initContainers {
				if initContainer.Name == "wait-gcs-ready" {
					assert.Equal(t, tc.expectedCmd, initContainer.Command)
				}
			}
		})
	}
}

// Check that autoscaler container overrides work as expected.
func TestBuildPodWithAutoscalerOptions(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	cluster.Spec.EnableInTreeAutoscaling = &trueFlag
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))

	customAutoscalerImage := "custom-autoscaler-xxx"
	customPullPolicy := corev1.PullIfNotPresent
	customTimeout := int32(100)
	customUpscaling := rayv1.UpscalingMode("Aggressive")
	customResources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: testMemoryLimit,
		},
		Limits: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("1"),
			corev1.ResourceMemory: testMemoryLimit,
		},
	}
	customEnv := []corev1.EnvVar{{Name: "fooEnv", Value: "fooValue"}}
	customEnvFrom := []corev1.EnvFromSource{{Prefix: "Pre"}}
	customVolumeMounts := []corev1.VolumeMount{
		{
			Name:      "ca-tls",
			MountPath: "/etc/ca/tls",
			ReadOnly:  true,
		},
		{
			Name:      "ray-tls",
			MountPath: "/etc/ray/tls",
		},
	}

	// Define a custom security profile.
	allowPrivilegeEscalation := false
	capabilities := corev1.Capabilities{
		Drop: []corev1.Capability{"ALL"},
	}
	seccompProfile := corev1.SeccompProfile{
		Type: corev1.SeccompProfileTypeRuntimeDefault,
	}
	runAsNonRoot := true
	customSecurityContext := corev1.SecurityContext{
		AllowPrivilegeEscalation: &allowPrivilegeEscalation,
		Capabilities:             &capabilities,
		RunAsNonRoot:             &runAsNonRoot,
		SeccompProfile:           &seccompProfile,
	}

	cluster.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{
		UpscalingMode:      &customUpscaling,
		IdleTimeoutSeconds: &customTimeout,
		Image:              &customAutoscalerImage,
		ImagePullPolicy:    &customPullPolicy,
		Resources:          &customResources,
		Env:                customEnv,
		EnvFrom:            customEnvFrom,
		VolumeMounts:       customVolumeMounts,
		SecurityContext:    &customSecurityContext,
	}
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod := BuildPod(ctx, podTemplateSpec, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", true, utils.GetCRDType(""), "", nil, "")
	expectedContainer := *autoscalerContainer.DeepCopy()
	expectedContainer.Image = customAutoscalerImage
	expectedContainer.ImagePullPolicy = customPullPolicy
	expectedContainer.Resources = customResources
	expectedContainer.EnvFrom = customEnvFrom
	expectedContainer.Env = append(expectedContainer.Env, customEnv...)
	expectedContainer.VolumeMounts = append(customVolumeMounts, expectedContainer.VolumeMounts...)
	expectedContainer.SecurityContext = &customSecurityContext
	index := getAutoscalerContainerIndex(pod)
	actualContainer := pod.Spec.Containers[index]
	assert.True(t, reflect.DeepEqual(expectedContainer, actualContainer))
}

func TestHeadPodTemplate_WithAutoscalingEnabled(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	cluster.Spec.EnableInTreeAutoscaling = &trueFlag
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")

	// autoscaler container is injected into head pod
	assert.Len(t, podTemplateSpec.Spec.Containers, 2)

	assert.Equal(t, cluster.Name, podTemplateSpec.Spec.ServiceAccountName)

	// Repeat ServiceAccountName check with long cluster name.
	cluster.Name = longString(t) // 200 chars long
	podTemplateSpec = DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	assert.Equal(t, shortString(t), podTemplateSpec.Spec.ServiceAccountName)
}

func TestDefaultHeadPodTemplate_Autoscaling(t *testing.T) {
	clusterNoAutoscaling := instance.DeepCopy()

	clusterAutoscalingVersionNotSet := instance.DeepCopy()
	clusterAutoscalingVersionNotSet.Spec.EnableInTreeAutoscaling = new(true)
	clusterAutoscalingV1 := instance.DeepCopy()
	clusterAutoscalingV1.Spec.EnableInTreeAutoscaling = new(true)
	clusterAutoscalingV1.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{
		Version: ptr.To(rayv1.AutoscalerVersionV1),
	}

	// clusterAutoscalingV2 has autoscaler V2 enabled: env var is injected AND RestartPolicy is set to Never.
	clusterAutoscalingV2 := instance.DeepCopy()
	clusterAutoscalingV2.Spec.EnableInTreeAutoscaling = new(true)
	clusterAutoscalingV2.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{
		Version: ptr.To(rayv1.AutoscalerVersionV2),
	}

	ctx := context.Background()
	podName := strings.ToLower(instance.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))

	// clusterAutoscalingV2WithCustomRestartPolicy has autoscaler V2 enabled and a custom RestartPolicy
	// set in the head pod template — used to verify the feature flag preserves the user value.
	clusterAutoscalingV2WithCustomRestartPolicy := clusterAutoscalingV2.DeepCopy()
	clusterAutoscalingV2WithCustomRestartPolicy.Spec.HeadGroupSpec.Template.Spec.RestartPolicy = corev1.RestartPolicyAlways

	tests := map[string]struct {
		expectedRestartPolicy      corev1.RestartPolicy
		cluster                    rayv1.RayCluster
		expectedHeadContainers     int
		expectedAutoscalerV2EnvVar bool
		expectedAutoscalerV1EnvVar bool
		rayVersion                 string
	}{
		"Pod template with autoscaling disabled should not have autoscaler container or other autoscaler related fields": {
			cluster:                    *clusterNoAutoscaling,
			expectedHeadContainers:     1,
			expectedAutoscalerV2EnvVar: false,
			expectedAutoscalerV1EnvVar: false,
			expectedRestartPolicy:      "",
		},
		"Pod template with autoscaling version not set should not have autoscaler env var": {
			cluster:                    *clusterAutoscalingVersionNotSet,
			expectedHeadContainers:     2,
			expectedAutoscalerV2EnvVar: false,
			expectedAutoscalerV1EnvVar: false,
			expectedRestartPolicy:      "",
		},
		"Pod template with autoscaling v1 enabled should have the correct autoscaler v1 fields": {
			cluster:                    *clusterAutoscalingV1,
			expectedHeadContainers:     2,
			expectedAutoscalerV2EnvVar: false,
			expectedAutoscalerV1EnvVar: true,
			expectedRestartPolicy:      "",
		},
		"Pod template with autoscaling v2 enabled and no Ray version should set RestartPolicy to Never": {
			cluster:                    *clusterAutoscalingV2,
			expectedHeadContainers:     2,
			expectedAutoscalerV2EnvVar: true,
			expectedAutoscalerV1EnvVar: false,
			expectedRestartPolicy:      corev1.RestartPolicyNever,
		},
		"Pod template with autoscaling v2 enabled and Ray < 2.56.0 should set RestartPolicy to Never": {
			cluster:                    *clusterAutoscalingV2,
			expectedHeadContainers:     2,
			expectedAutoscalerV2EnvVar: true,
			expectedAutoscalerV1EnvVar: false,
			rayVersion:                 "2.55.0",
			expectedRestartPolicy:      corev1.RestartPolicyNever,
		},
		"Pod template with autoscaling v2 enabled and Ray >= 2.56.0 should not force RestartPolicy to Never": {
			cluster:                    *clusterAutoscalingV2,
			expectedHeadContainers:     2,
			expectedAutoscalerV2EnvVar: true,
			expectedAutoscalerV1EnvVar: false,
			rayVersion:                 "2.56.0",
			// RestartPolicy is not set in the cluster template, so it should remain empty.
			expectedRestartPolicy: "",
		},
		"Pod template with autoscaling v2 enabled and Ray >= 2.56.0 should preserve user-set RestartPolicy": {
			cluster:                    *clusterAutoscalingV2WithCustomRestartPolicy,
			expectedHeadContainers:     2,
			expectedAutoscalerV2EnvVar: true,
			expectedAutoscalerV1EnvVar: false,
			rayVersion:                 "2.56.0",
			// RestartPolicy is set to Always in the cluster template and should be kept as-is.
			expectedRestartPolicy: corev1.RestartPolicyAlways,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cluster := tc.cluster.DeepCopy()
			cluster.Spec.RayVersion = tc.rayVersion
			podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")

			// if autoscaling is enabled, the head pod should have the autoscaler container appended for a total of 2 containers
			if utils.IsAutoscalingEnabled(&cluster.Spec) {
				assert.Len(t, podTemplateSpec.Spec.Containers, tc.expectedHeadContainers)
			}

			if tc.expectedAutoscalerV2EnvVar {
				assert.Contains(t, podTemplateSpec.Spec.Containers[0].Env, corev1.EnvVar{
					Name:  utils.RAY_ENABLE_AUTOSCALER_V2,
					Value: "true",
				})
			} else {
				assert.NotContains(t, podTemplateSpec.Spec.Containers[0].Env, corev1.EnvVar{
					Name:  utils.RAY_ENABLE_AUTOSCALER_V2,
					Value: "true",
				})
			}

			if tc.expectedAutoscalerV1EnvVar {
				assert.Contains(t, podTemplateSpec.Spec.Containers[0].Env, corev1.EnvVar{
					Name:  utils.RAY_ENABLE_AUTOSCALER_V2,
					Value: "false",
				})
			} else {
				assert.NotContains(t, podTemplateSpec.Spec.Containers[0].Env, corev1.EnvVar{
					Name:  utils.RAY_ENABLE_AUTOSCALER_V2,
					Value: "false",
				})
			}

			assert.Equal(t, tc.expectedRestartPolicy, podTemplateSpec.Spec.RestartPolicy)
		})
	}
}

func TestHeadPodTemplate_AutoscalerImage(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	cluster.Spec.EnableInTreeAutoscaling = &trueFlag
	cluster.Spec.AutoscalerOptions = nil
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))

	// Case 1: If `AutoscalerOptions.Image` is not set, the Autoscaler container should use the Ray head container's image by default.
	expectedAutoscalerImage := cluster.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Image
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod := corev1.Pod{
		Spec: podTemplateSpec.Spec,
	}
	autoscalerContainerIndex := getAutoscalerContainerIndex(pod)
	assert.Equal(t, expectedAutoscalerImage, podTemplateSpec.Spec.Containers[autoscalerContainerIndex].Image)

	// Case 2: If `AutoscalerOptions.Image` is set, the Autoscaler container should use the specified image.
	customAutoscalerImage := "custom-autoscaler-xxx"
	cluster = instance.DeepCopy()
	cluster.Spec.EnableInTreeAutoscaling = &trueFlag
	cluster.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{
		Image: &customAutoscalerImage,
	}
	podTemplateSpec = DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	pod.Spec = podTemplateSpec.Spec
	autoscalerContainerIndex = getAutoscalerContainerIndex(pod)
	assert.Equal(t, customAutoscalerImage, podTemplateSpec.Spec.Containers[autoscalerContainerIndex].Image)
}

// If no service account is specified in the RayCluster,
// the head pod's service account should be an empty string.
func TestHeadPodTemplate_WithNoServiceAccount(t *testing.T) {
	cluster := instance.DeepCopy()
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	pod := DefaultHeadPodTemplate(context.Background(), *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")

	assert.Empty(t, pod.Spec.ServiceAccountName)
}

// If a service account is specified in the RayCluster and EnableInTreeAutoscaling is set to false,
// the head pod's service account should be the same.
func TestHeadPodTemplate_WithServiceAccountNoAutoscaling(t *testing.T) {
	cluster := instance.DeepCopy()
	serviceAccount := "head-service-account"
	cluster.Spec.HeadGroupSpec.Template.Spec.ServiceAccountName = serviceAccount
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	pod := DefaultHeadPodTemplate(context.Background(), *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")

	assert.Equal(t, serviceAccount, pod.Spec.ServiceAccountName)
}

// If a service account is specified in the RayCluster and EnableInTreeAutoscaling is set to true,
// the head pod's service account should be the same.
func TestHeadPodTemplate_WithServiceAccount(t *testing.T) {
	cluster := instance.DeepCopy()
	serviceAccount := "head-service-account"
	cluster.Spec.HeadGroupSpec.Template.Spec.ServiceAccountName = serviceAccount
	cluster.Spec.EnableInTreeAutoscaling = &trueFlag
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	pod := DefaultHeadPodTemplate(context.Background(), *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")

	assert.Equal(t, serviceAccount, pod.Spec.ServiceAccountName)
}

func splitAndSort(s string) []string {
	strs := strings.Split(s, " ")
	result := make([]string, 0, len(strs))
	for _, s := range strs {
		if len(s) > 0 {
			result = append(result, s)
		}
	}
	sort.Strings(result)
	return result
}

func TestDefaultWorkerPodTemplateWithName(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	worker := cluster.Spec.WorkerGroupSpecs[0]
	worker.Template.ObjectMeta.Name = "ray-worker-test"
	podName := cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
	expectedWorker := *worker.DeepCopy()

	// Pass a deep copy of worker (*worker.DeepCopy()) to prevent "worker" from updating.
	podTemplateSpec := DefaultWorkerPodTemplate(ctx, *cluster, *worker.DeepCopy(), podName, fqdnRayIP, "6379", "", 0, 0)
	assert.Empty(t, podTemplateSpec.ObjectMeta.Name)
	assert.Equal(t, expectedWorker, worker)
}

func TestDeafultWorkerPodTemplateWithReplicaGrpAndIndex(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()

	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	worker := cluster.Spec.WorkerGroupSpecs[0]

	features.SetFeatureGateDuringTest(t, features.RayMultiHostIndexing, true)

	worker.Template.ObjectMeta.Name = "ray-worker-test"
	worker.NumOfHosts = 4
	podName := cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
	groupReplicaName := utils.GenerateRayWorkerReplicaGroupName(worker.GroupName)

	// Pass a deep copy of worker (*worker.DeepCopy()) to prevent "worker" from updating.
	podTemplateSpec := DefaultWorkerPodTemplate(ctx, *cluster, *worker.DeepCopy(), podName, fqdnRayIP, "6379", groupReplicaName, 0, 2)
	assert.Empty(t, podTemplateSpec.ObjectMeta.Name)
	assert.Equal(t, podTemplateSpec.Labels[utils.RayWorkerReplicaNameKey], groupReplicaName)
	assert.Equal(t, "0", podTemplateSpec.Labels[utils.RayWorkerReplicaIndexKey])
	assert.Equal(t, "2", podTemplateSpec.Labels[utils.RayHostIndexKey])
}

func containerPortExists(ports []corev1.ContainerPort, containerPort int32) error {
	name := utils.MetricsPortName
	for _, port := range ports {
		if port.Name == name {
			if port.ContainerPort != containerPort {
				return fmt.Errorf("expected `%v` but got `%v` for `%v` port", containerPort, port.ContainerPort, name)
			}
			return nil
		}
	}
	return fmt.Errorf("couldn't find `%v` port", name)
}

func TestDefaultHeadPodTemplateWithConfigurablePorts(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	cluster.Spec.HeadGroupSpec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{}
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	podTemplateSpec := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	// DefaultHeadPodTemplate will add the default metrics port if user doesn't specify it.
	// Verify the default metrics port exists.
	require.NoError(t, containerPortExists(podTemplateSpec.Spec.Containers[0].Ports, int32(utils.DefaultMetricsPort)))
	customMetricsPort := int32(utils.DefaultMetricsPort) + 1
	metricsPort := corev1.ContainerPort{
		Name:          utils.MetricsPortName,
		ContainerPort: customMetricsPort,
	}
	cluster.Spec.HeadGroupSpec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{metricsPort}
	podTemplateSpec = DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	// Verify the custom metrics port exists.
	require.NoError(t, containerPortExists(podTemplateSpec.Spec.Containers[0].Ports, customMetricsPort))
}

func TestDefaultWorkerPodTemplateWithConfigurablePorts(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	cluster.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Ports = []corev1.ContainerPort{}
	worker := cluster.Spec.WorkerGroupSpecs[0]
	podName := cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	podTemplateSpec := DefaultWorkerPodTemplate(ctx, *cluster, worker, podName, fqdnRayIP, "6379", "", 0, 0)
	// DefaultWorkerPodTemplate will add the default metrics port if user doesn't specify it.
	// Verify the default metrics port exists.
	require.NoError(t, containerPortExists(podTemplateSpec.Spec.Containers[0].Ports, int32(utils.DefaultMetricsPort)))
	customMetricsPort := int32(utils.DefaultMetricsPort) + 1
	metricsPort := corev1.ContainerPort{
		Name:          utils.MetricsPortName,
		ContainerPort: customMetricsPort,
	}
	cluster.Spec.WorkerGroupSpecs[0].Template.Spec.Containers[0].Ports = []corev1.ContainerPort{metricsPort}
	podTemplateSpec = DefaultWorkerPodTemplate(ctx, *cluster, worker, podName, fqdnRayIP, "6379", "", 0, 0)
	// Verify the custom metrics port exists.
	require.NoError(t, containerPortExists(podTemplateSpec.Spec.Containers[0].Ports, customMetricsPort))
}

func TestDefaultWorkerPodTemplate_Autoscaling(t *testing.T) {
	clusterNoAutoscaling := instance.DeepCopy()
	clusterAutoscalingV1 := instance.DeepCopy()
	clusterAutoscalingV1.Spec.EnableInTreeAutoscaling = new(true)
	clusterAutoscalingV1.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{
		Version: ptr.To(rayv1.AutoscalerVersionV1),
	}
	// v1 with a RestartPolicy already set by the user — should not be overridden.
	clusterAutoscalingV1WithRestartPolicy := clusterAutoscalingV1.DeepCopy()
	clusterAutoscalingV1WithRestartPolicy.Spec.WorkerGroupSpecs[0].Template.Spec.RestartPolicy = corev1.RestartPolicyAlways
	clusterAutoscalingV2 := instance.DeepCopy()
	clusterAutoscalingV2.Spec.EnableInTreeAutoscaling = new(true)
	clusterAutoscalingV2.Spec.AutoscalerOptions = &rayv1.AutoscalerOptions{
		Version: ptr.To(rayv1.AutoscalerVersionV2),
	}
	// Autoscaling enabled but no version set — v2 is the default.
	clusterAutoscalingVersionNotSet := instance.DeepCopy()
	clusterAutoscalingVersionNotSet.Spec.EnableInTreeAutoscaling = new(true)

	ctx := context.Background()
	podName := strings.ToLower(instance.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + utils.FormatInt32(0))
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, instance, instance.Namespace)

	// clusterAutoscalingV2WithCustomRestartPolicy has a non-Never RestartPolicy set on the first
	// worker group template — used to verify the feature flag preserves the user value.
	clusterAutoscalingV2WithWorkerCustomRestartPolicy := clusterAutoscalingV2.DeepCopy()
	clusterAutoscalingV2WithWorkerCustomRestartPolicy.Spec.WorkerGroupSpecs[0].Template.Spec.RestartPolicy = corev1.RestartPolicyAlways

	tests := map[string]struct {
		expectedRestartPolicy corev1.RestartPolicy
		cluster               rayv1.RayCluster
		rayVersion            string
	}{
		"Pod template with autoscaling disabled should not have autoscaler container or other autoscaler related fields": {
			cluster:               *clusterNoAutoscaling,
			expectedRestartPolicy: "",
		},
		"Pod template with autoscaling v1 enabled and no RestartPolicy set should not set RestartPolicy": {
			cluster:               *clusterAutoscalingV1,
			expectedRestartPolicy: "",
		},
		"Pod template with autoscaling v1 enabled and RestartPolicy already set should not override it": {
			cluster:               *clusterAutoscalingV1WithRestartPolicy,
			expectedRestartPolicy: corev1.RestartPolicyAlways,
		},
		"Pod template with autoscaling v2 enabled and no Ray version should set RestartPolicy to Never": {
			cluster:               *clusterAutoscalingV2,
			expectedRestartPolicy: corev1.RestartPolicyNever,
		},
		"Pod template with autoscaling v2 enabled and Ray < 2.56.0 should set RestartPolicy to Never": {
			cluster:               *clusterAutoscalingV2,
			rayVersion:            "2.55.0",
			expectedRestartPolicy: corev1.RestartPolicyNever,
		},
		"Pod template with autoscaling v2 enabled and Ray >= 2.56.0 should not force RestartPolicy to Never": {
			cluster:    *clusterAutoscalingV2,
			rayVersion: "2.56.0",
			// RestartPolicy is not set in the worker template, so it should remain empty.
			expectedRestartPolicy: "",
		},
		"Pod template with autoscaling v2 enabled and Ray >= 2.56.0 should preserve user-set RestartPolicy": {
			cluster:    *clusterAutoscalingV2WithWorkerCustomRestartPolicy,
			rayVersion: "2.56.0",
			// RestartPolicy is set to Always in the worker template and should be kept as-is.
			expectedRestartPolicy: corev1.RestartPolicyAlways,
		},
		"Pod template with autoscaling enabled and version not set should not set RestartPolicy": {
			cluster:               *clusterAutoscalingVersionNotSet,
			expectedRestartPolicy: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			cluster := tc.cluster.DeepCopy()
			cluster.Spec.RayVersion = tc.rayVersion
			podTemplateSpec := DefaultWorkerPodTemplate(ctx, *cluster, cluster.Spec.WorkerGroupSpecs[0], podName, fqdnRayIP, "6379", "", 0, 0)
			assert.Equal(t, tc.expectedRestartPolicy, podTemplateSpec.Spec.RestartPolicy)
		})
	}
}

func TestDefaultInitContainer(t *testing.T) {
	ctx := context.Background()
	// A default init container to check the health of GCS is expected to be added.
	cluster := instance.DeepCopy()
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	worker := cluster.Spec.WorkerGroupSpecs[0]
	podName := cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
	expectedResult := len(cluster.Spec.WorkerGroupSpecs[0].Template.Spec.InitContainers) + 1

	// Pass a deep copy of worker (*worker.DeepCopy()) to prevent "worker" from updating.
	podTemplateSpec := DefaultWorkerPodTemplate(ctx, *cluster, *worker.DeepCopy(), podName, fqdnRayIP, "6379", "", 0, 0)
	numInitContainers := len(podTemplateSpec.Spec.InitContainers)
	assert.Equal(t, expectedResult, numInitContainers, "A default init container is expected to be added.")

	// This health-check init container requires certain environment variables to establish a secure connection
	// with the Ray head using TLS authentication. Currently, we simply copied all environment variables from
	// Ray container to the init container. This may be changed in the future.
	healthCheckContainer := podTemplateSpec.Spec.InitContainers[numInitContainers-1]
	rayContainer := worker.Template.Spec.Containers[utils.RayContainerIndex]

	assert.NotEmpty(t, rayContainer.Env, "The test only makes sense if the Ray container has environment variables.")
	assert.Len(t, healthCheckContainer.Env, len(rayContainer.Env))
	for _, env := range rayContainer.Env {
		// env.ValueFrom is the source for the environment variable's value. Cannot be used if value is not empty.
		if env.Value != "" {
			checkContainerEnv(t, healthCheckContainer, env.Name, env.Value)
		} else {
			checkContainerEnv(t, healthCheckContainer, env.Name, env.ValueFrom.FieldRef.FieldPath)
		}
	}

	assert.NotEmpty(t, rayContainer.Resources, "The test only makes sense if the Ray container has resource limit/request.")
}

func TestDefaultInitContainerImagePullPolicy(t *testing.T) {
	ctx := context.Background()

	cluster := instance.DeepCopy()
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	worker := cluster.Spec.WorkerGroupSpecs[0]
	podName := cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)

	cases := []struct {
		name               string
		imagePullPolicy    corev1.PullPolicy
		expectedPullPolicy corev1.PullPolicy
	}{
		{
			name:               "Always",
			imagePullPolicy:    corev1.PullAlways,
			expectedPullPolicy: corev1.PullAlways,
		},
		{
			name:               "IfNotPresent",
			imagePullPolicy:    corev1.PullIfNotPresent,
			expectedPullPolicy: corev1.PullIfNotPresent,
		},
		{
			name:               "Never",
			imagePullPolicy:    corev1.PullNever,
			expectedPullPolicy: corev1.PullNever,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// set ray container imagePullPolicy
			worker.Template.Spec.Containers[utils.RayContainerIndex].ImagePullPolicy = tc.imagePullPolicy

			podTemplateSpec := DefaultWorkerPodTemplate(ctx, *cluster, *worker.DeepCopy(), podName, fqdnRayIP, "6379", "", 0, 0)

			healthCheckContainer := podTemplateSpec.Spec.InitContainers[len(podTemplateSpec.Spec.InitContainers)-1]
			assert.Equal(t, tc.expectedPullPolicy, healthCheckContainer.ImagePullPolicy, "The ImagePullPolicy of the init container should be the same as the Ray container.")
		})
	}
}

func TestSetMissingRayStartParamsAddress(t *testing.T) {
	ctx := context.Background()
	// The address option is automatically injected into RayStartParams with a default value of <Fully Qualified Domain Name (FQDN)>:<headPort> for workers only, which is used to connect to the Ray cluster.
	// The head should not include the address option in RayStartParams, as it can access the GCS server, which also runs on the head, via localhost.
	// Although not recommended, users can manually set the address option for both head and workers.

	headPort := "6379"
	fqdnRayIP := "raycluster-kuberay-head-svc.default.svc.cluster.local"
	customAddress := "custom-address:1234"

	testCases := []struct {
		name           string
		assertion      func(t *testing.T, rayStartParams map[string]string)
		rayStartParams map[string]string
		fqdnRayIP      string
		nodeType       rayv1.RayNodeType
	}{
		{
			name:           "Head node with no address option set.",
			rayStartParams: map[string]string{},
			fqdnRayIP:      "",
			nodeType:       rayv1.HeadNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.NotContains(t, rayStartParams, "address", "Head node should not have an address option set by default.")
			},
		},
		{
			name:           "Head node with custom address option set.",
			rayStartParams: map[string]string{"address": customAddress},
			fqdnRayIP:      "",
			nodeType:       rayv1.HeadNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, customAddress, rayStartParams["address"], "Expected `%v` but got `%v`", customAddress, rayStartParams["address"])
			},
		},
		{
			name:           "Worker node with no address option set.",
			rayStartParams: map[string]string{},
			fqdnRayIP:      fqdnRayIP,
			nodeType:       rayv1.WorkerNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				expectedAddress := fmt.Sprintf("%s:%s", fqdnRayIP, headPort)
				assert.Equalf(t, expectedAddress, rayStartParams["address"], "Expected `%v` but got `%v`", expectedAddress, rayStartParams["address"])
			},
		},
		{
			name:           "Worker node with custom address option set.",
			rayStartParams: map[string]string{"address": customAddress},
			fqdnRayIP:      fqdnRayIP,
			nodeType:       rayv1.WorkerNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, customAddress, rayStartParams["address"], "Expected `%v` but got `%v`", customAddress, rayStartParams["address"])
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.rayStartParams = setMissingRayStartParams(ctx, testCase.rayStartParams, testCase.nodeType, headPort, testCase.fqdnRayIP)
			testCase.assertion(t, testCase.rayStartParams)
		})
	}
}

func TestSetMissingRayStartParamsMetricsExportPort(t *testing.T) {
	ctx := context.Background()

	// The metrics-export-port option is automatically injected into RayStartParams with a default value of DefaultMetricsPort for both head and workers.
	// Users can manually set the metrics-export-port option to customize the metrics export port and scrape Ray’s metrics using Prometheus via <ip>:<custom metrics export port>.
	// See https://github.com/ray-project/kuberay/pull/954 for more details.

	headPort := "6379"
	fqdnRayIP := "raycluster-kuberay-head-svc.default.svc.cluster.local"
	customMetricsPort := utils.DefaultMetricsPort + 1
	testCases := []struct {
		name           string
		assertion      func(t *testing.T, rayStartParams map[string]string)
		rayStartParams map[string]string
		fqdnRayIP      string
		nodeType       rayv1.RayNodeType
	}{
		{
			name:           "Head node with no metrics-export-port option set.",
			rayStartParams: map[string]string{},
			fqdnRayIP:      "",
			nodeType:       rayv1.HeadNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, fmt.Sprint(utils.DefaultMetricsPort), rayStartParams["metrics-export-port"], "Expected `%v` but got `%v`", utils.DefaultMetricsPort, rayStartParams["metrics-export-port"])
			},
		},
		{
			name:           "Head node with custom metrics-export-port option set.",
			rayStartParams: map[string]string{"metrics-export-port": fmt.Sprint(customMetricsPort)},
			fqdnRayIP:      "",
			nodeType:       rayv1.HeadNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, fmt.Sprint(customMetricsPort), rayStartParams["metrics-export-port"], "Expected `%v` but got `%v`", customMetricsPort, rayStartParams["metrics-export-port"])
			},
		},
		{
			name:           "Worker node with no metrics-export-port option set.",
			rayStartParams: map[string]string{},
			fqdnRayIP:      fqdnRayIP,
			nodeType:       rayv1.WorkerNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, fmt.Sprint(utils.DefaultMetricsPort), rayStartParams["metrics-export-port"], "Expected `%v` but got `%v`", utils.DefaultMetricsPort, rayStartParams["metrics-export-port"])
			},
		},
		{
			name:           "Worker node with custom metrics-export-port option set.",
			rayStartParams: map[string]string{"metrics-export-port": fmt.Sprint(customMetricsPort)},
			fqdnRayIP:      fqdnRayIP,
			nodeType:       rayv1.WorkerNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, fmt.Sprint(customMetricsPort), rayStartParams["metrics-export-port"], "Expected `%v` but got `%v`", customMetricsPort, rayStartParams["metrics-export-port"])
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.rayStartParams = setMissingRayStartParams(ctx, testCase.rayStartParams, testCase.nodeType, headPort, testCase.fqdnRayIP)
			testCase.assertion(t, testCase.rayStartParams)
		})
	}
}

func TestSetMissingRayStartParamsBlock(t *testing.T) {
	ctx := context.Background()

	// The --block option is automatically injected into RayStartParams with a default value of true for both head and workers.
	// Although not recommended, users can manually set the --block option.
	// See https://github.com/ray-project/kuberay/pull/675 for more details.

	headPort := "6379"
	fqdnRayIP := "raycluster-kuberay-head-svc.default.svc.cluster.local"
	testCases := []struct {
		name           string
		assertion      func(t *testing.T, rayStartParams map[string]string)
		rayStartParams map[string]string
		fqdnRayIP      string
		nodeType       rayv1.RayNodeType
	}{
		{
			name:           "Head node with no --block option set.",
			rayStartParams: map[string]string{},
			fqdnRayIP:      "",
			nodeType:       rayv1.HeadNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, "true", rayStartParams["block"], "Expected `%v` but got `%v`", "true", rayStartParams["block"])
			},
		},
		{
			name:           "Head node with --block option set to false.",
			rayStartParams: map[string]string{"block": "false"},
			fqdnRayIP:      "",
			nodeType:       rayv1.HeadNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, "true", rayStartParams["block"], "Expected `%v` but got `%v`", "false", rayStartParams["block"])
			},
		},
		{
			name:           "Worker node with no --block option set.",
			rayStartParams: map[string]string{},
			fqdnRayIP:      fqdnRayIP,
			nodeType:       rayv1.WorkerNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, "true", rayStartParams["block"], "Expected `%v` but got `%v`", "true", rayStartParams["block"])
			},
		},
		{
			name:           "Worker node with --block option set to false.",
			rayStartParams: map[string]string{"block": "false"},
			fqdnRayIP:      fqdnRayIP,
			nodeType:       rayv1.WorkerNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, "true", rayStartParams["block"], "Expected `%v` but got `%v`", "false", rayStartParams["block"])
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.rayStartParams = setMissingRayStartParams(ctx, testCase.rayStartParams, testCase.nodeType, headPort, testCase.fqdnRayIP)
			testCase.assertion(t, testCase.rayStartParams)
		})
	}
}

func TestSetMissingRayStartParamsDashboardHost(t *testing.T) {
	ctx := context.Background()

	// The dashboard-host option is automatically injected into RayStartParams with a default value of "0.0.0.0" for head only as workers do not have dashborad server.
	// Users can manually set the dashboard-host option to customize the host the dashboard server binds to, either "localhost" (127.0.0.1) or "0.0.0.0" (available from all interfaces).
	headPort := "6379"
	fqdnRayIP := "raycluster-kuberay-head-svc.default.svc.cluster.local"
	testCases := []struct {
		name           string
		assertion      func(t *testing.T, rayStartParams map[string]string)
		rayStartParams map[string]string
		fqdnRayIP      string
		nodeType       rayv1.RayNodeType
	}{
		{
			name:           "Head node with no dashboard-host option set.",
			rayStartParams: map[string]string{},
			fqdnRayIP:      "",
			nodeType:       rayv1.HeadNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, "0.0.0.0", rayStartParams["dashboard-host"], "Expected `%v` but got `%v`", "0.0.0.0", rayStartParams["dashboard-host"])
			},
		},
		{
			name:           "Head node with dashboard-host option set.",
			rayStartParams: map[string]string{"dashboard-host": "localhost"},
			fqdnRayIP:      "",
			nodeType:       rayv1.HeadNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, "localhost", rayStartParams["dashboard-host"], "Expected `%v` but got `%v`", "localhost", rayStartParams["dashboard-host"])
			},
		},
		{
			name:           "Worker node with no dashboard-host option set.",
			rayStartParams: map[string]string{},
			fqdnRayIP:      fqdnRayIP,
			nodeType:       rayv1.WorkerNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.NotContains(t, rayStartParams, "dashboard-host", "workers should not have an dashboard-host option set.")
			},
		},
		{
			name:           "Worker node with dashboard-host option set.",
			rayStartParams: map[string]string{"dashboard-host": "localhost"},
			fqdnRayIP:      fqdnRayIP,
			nodeType:       rayv1.WorkerNode,
			assertion: func(t *testing.T, rayStartParams map[string]string) {
				assert.Equalf(t, "localhost", rayStartParams["dashboard-host"], "Expected `%v` but got `%v`", "localhost", rayStartParams["dashboard-host"])
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			testCase.rayStartParams = setMissingRayStartParams(ctx, testCase.rayStartParams, testCase.nodeType, headPort, testCase.fqdnRayIP)
			testCase.assertion(t, testCase.rayStartParams)
		})
	}
}

func TestGetCustomWorkerInitImage(t *testing.T) {
	// cleanup
	defer os.Unsetenv(EnableInitContainerInjectionEnvKey)

	// not set the env
	b := getEnableInitContainerInjection()
	assert.True(t, b)
	// set the env with "true"
	os.Setenv(EnableInitContainerInjectionEnvKey, "true")
	b = getEnableInitContainerInjection()
	assert.True(t, b)
	// set the env with "True"
	os.Setenv(EnableInitContainerInjectionEnvKey, "True")
	b = getEnableInitContainerInjection()
	assert.True(t, b)
	// set the env with "false"
	os.Setenv(EnableInitContainerInjectionEnvKey, "false")
	b = getEnableInitContainerInjection()
	assert.False(t, b)
	// set the env with "False"
	os.Setenv(EnableInitContainerInjectionEnvKey, "False")
	b = getEnableInitContainerInjection()
	assert.False(t, b)
}

func TestGetEnableProbesInjection(t *testing.T) {
	// cleanup
	defer os.Unsetenv(utils.ENABLE_PROBES_INJECTION)

	// not set the env
	os.Unsetenv(utils.ENABLE_PROBES_INJECTION)
	b := getEnableProbesInjection()
	assert.True(t, b)
	// set the env with "true"
	os.Setenv(utils.ENABLE_PROBES_INJECTION, "true")
	b = getEnableProbesInjection()
	assert.True(t, b)
	// set the env with "True"
	os.Setenv(utils.ENABLE_PROBES_INJECTION, "True")
	b = getEnableProbesInjection()
	assert.True(t, b)
	// set the env with "false"
	os.Setenv(utils.ENABLE_PROBES_INJECTION, "false")
	b = getEnableProbesInjection()
	assert.False(t, b)
	// set the env with "False"
	os.Setenv(utils.ENABLE_PROBES_INJECTION, "False")
	b = getEnableProbesInjection()
	assert.False(t, b)
}

func TestInitLivenessAndReadinessProbe(t *testing.T) {
	cluster := instance.DeepCopy()
	podName := strings.ToLower(cluster.Name + utils.DashSymbol + string(rayv1.HeadNode) + utils.DashSymbol + utils.FormatInt32(0))
	podTemplateSpec := DefaultHeadPodTemplate(context.Background(), *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")
	rayContainer := &podTemplateSpec.Spec.Containers[utils.RayContainerIndex]
	rayStartParams := make(map[string]string)

	// Test 1: User defines a custom HTTPGet probe.
	httpGetProbe := corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			HTTPGet: &corev1.HTTPGetAction{
				// Check Raylet status
				Path: fmt.Sprintf("/%s", utils.RayAgentRayletHealthPath),
				Port: intstr.FromInt(utils.DefaultDashboardAgentListenPort),
			},
		},
	}

	rayContainer.LivenessProbe = &httpGetProbe
	rayContainer.ReadinessProbe = &httpGetProbe
	initLivenessAndReadinessProbe(rayContainer, rayv1.HeadNode, "", rayStartParams, "")
	assert.NotNil(t, rayContainer.LivenessProbe.HTTPGet)
	assert.NotNil(t, rayContainer.ReadinessProbe.HTTPGet)
	assert.Nil(t, rayContainer.LivenessProbe.Exec)
	assert.Nil(t, rayContainer.ReadinessProbe.Exec)

	// Test 2: User does not define a custom probe. KubeRay will inject Exec probe for worker pod.
	// Here we test the case where the Ray Pod originates from RayServiceCRD,
	// implying that an additional serve health check will be added to the readiness probe.
	rayContainer.LivenessProbe = nil
	rayContainer.ReadinessProbe = nil
	initLivenessAndReadinessProbe(rayContainer, rayv1.WorkerNode, utils.RayServiceCRD, rayStartParams, "")
	assert.NotNil(t, rayContainer.LivenessProbe.Exec)
	assert.NotNil(t, rayContainer.ReadinessProbe.Exec)
	assert.NotContains(t, strings.Join(rayContainer.LivenessProbe.Exec.Command, " "), utils.RayServeProxyHealthPath)
	assert.Contains(t, strings.Join(rayContainer.ReadinessProbe.Exec.Command, " "), utils.RayServeProxyHealthPath)
	assert.Equal(t, int32(2), rayContainer.LivenessProbe.TimeoutSeconds)
	assert.Equal(t, int32(2), rayContainer.ReadinessProbe.TimeoutSeconds)

	// Test 3: User does not define a custom probe. KubeRay will inject Exec probe for head pod.
	// Here we test the case where the Ray Pod originates from RayServiceCRD,
	// implying that an additional serve health check will be added to the readiness probe.
	rayContainer.LivenessProbe = nil
	rayContainer.ReadinessProbe = nil
	initLivenessAndReadinessProbe(rayContainer, rayv1.HeadNode, utils.RayServiceCRD, rayStartParams, "")
	assert.NotNil(t, rayContainer.LivenessProbe.Exec)
	assert.NotNil(t, rayContainer.ReadinessProbe.Exec)
	// head pod should not have Ray Serve proxy health probes
	assert.NotContains(t, strings.Join(rayContainer.LivenessProbe.Exec.Command, " "), utils.RayServeProxyHealthPath)
	assert.NotContains(t, strings.Join(rayContainer.ReadinessProbe.Exec.Command, " "), utils.RayServeProxyHealthPath)
	assert.Equal(t, int32(5), rayContainer.LivenessProbe.TimeoutSeconds)
	assert.Equal(t, int32(5), rayContainer.ReadinessProbe.TimeoutSeconds)

	// Test 4: Test custom ports in rayStartParams for head node.
	rayContainer.LivenessProbe = nil
	rayContainer.ReadinessProbe = nil
	customRayStartParams := map[string]string{
		"dashboard-agent-listen-port": "8266",
		"dashboard-port":              "8365",
	}
	initLivenessAndReadinessProbe(rayContainer, rayv1.HeadNode, utils.RayClusterCRD, customRayStartParams, "")
	assert.NotNil(t, rayContainer.LivenessProbe.Exec)
	assert.NotNil(t, rayContainer.ReadinessProbe.Exec)

	livenessCommand := strings.Join(rayContainer.LivenessProbe.Exec.Command, " ")
	readinessCommand := strings.Join(rayContainer.ReadinessProbe.Exec.Command, " ")

	assert.Contains(t, livenessCommand, ":8266", "Head pod liveness probe should use custom dashboard-agent-listen-port")
	assert.Contains(t, livenessCommand, ":8365", "Head pod liveness probe should use custom dashboard-port")
	assert.Contains(t, readinessCommand, ":8266", "Head pod readiness probe should use custom dashboard-agent-listen-port")
	assert.Contains(t, readinessCommand, ":8365", "Head pod readiness probe should use custom dashboard-port")

	// Test 5: Test custom ports in rayStartParams for worker node
	rayContainer.LivenessProbe = nil
	rayContainer.ReadinessProbe = nil
	workerRayStartParams := map[string]string{
		"dashboard-agent-listen-port": "9000",
	}
	initLivenessAndReadinessProbe(rayContainer, rayv1.WorkerNode, utils.RayClusterCRD, workerRayStartParams, "")
	assert.NotNil(t, rayContainer.LivenessProbe.Exec)
	assert.NotNil(t, rayContainer.ReadinessProbe.Exec)

	workerLivenessCommand := strings.Join(rayContainer.LivenessProbe.Exec.Command, " ")
	workerReadinessCommand := strings.Join(rayContainer.ReadinessProbe.Exec.Command, " ")

	assert.Contains(t, workerLivenessCommand, ":9000", "Worker pod should use custom dashboard-agent-listen-port")
	assert.Contains(t, workerReadinessCommand, ":9000", "Worker pod should use custom dashboard-agent-listen-port")
	assert.NotContains(t, workerLivenessCommand, fmt.Sprintf(":%d", utils.DefaultDashboardPort), "Worker pod should not check dashboard-port")
	assert.NotContains(t, workerReadinessCommand, fmt.Sprintf(":%d", utils.DefaultDashboardPort), "Worker pod should not check dashboard-port")

	// Test 6: Test RayService worker with custom ports and serve proxy health check
	rayContainer.LivenessProbe = nil
	rayContainer.ReadinessProbe = nil
	rayContainer.Ports = []corev1.ContainerPort{
		{
			Name:          utils.ServingPortName,
			ContainerPort: int32(utils.DefaultServingPort),
		},
	}
	rayServiceWorkerParams := map[string]string{
		"dashboard-agent-listen-port": "8500",
	}
	initLivenessAndReadinessProbe(rayContainer, rayv1.WorkerNode, utils.RayServiceCRD, rayServiceWorkerParams, "")
	rayServiceReadinessCommand := strings.Join(rayContainer.ReadinessProbe.Exec.Command, " ")
	assert.Contains(t, rayServiceReadinessCommand, ":8500", "RayService worker should use custom dashboard-agent-listen-port")
	assert.Contains(t, rayServiceReadinessCommand, utils.RayServeProxyHealthPath, "RayService worker should include serve proxy health check")
	assert.Equal(t, int32(utils.ServeReadinessProbeFailureThreshold), rayContainer.ReadinessProbe.FailureThreshold, "RayService worker should have correct failure threshold")

	// Test 8: Test invalid port values (should fall back to defaults)
	rayContainer.LivenessProbe = nil
	rayContainer.ReadinessProbe = nil
	invalidPortParams := map[string]string{
		"dashboard-agent-listen-port": "invalid-port",
		"dashboard-port":              "not-a-number",
	}
	initLivenessAndReadinessProbe(rayContainer, rayv1.HeadNode, utils.RayClusterCRD, invalidPortParams, "")

	invalidPortLivenessCommand := strings.Join(rayContainer.LivenessProbe.Exec.Command, " ")

	// Should fall back to default ports when invalid values are provided
	assert.Contains(t, invalidPortLivenessCommand, fmt.Sprintf(":%d", utils.DefaultDashboardAgentListenPort), "Should fall back to default dashboard-agent-listen-port for invalid input")
	assert.Contains(t, invalidPortLivenessCommand, fmt.Sprintf(":%d", utils.DefaultDashboardPort), "Should fall back to default dashboard-port for invalid input")

	// Test 9: Test unified health check at ray version 2.53.0 and later.
	rayContainer.LivenessProbe = nil
	rayContainer.ReadinessProbe = nil
	// Verify head node has http probes and no exec probes.
	initLivenessAndReadinessProbe(rayContainer, rayv1.HeadNode, utils.RayClusterCRD, rayStartParams, "2.53.0")
	assert.NotNil(t, rayContainer.LivenessProbe.HTTPGet)
	assert.NotNil(t, rayContainer.ReadinessProbe.HTTPGet)
	assert.Nil(t, rayContainer.LivenessProbe.Exec)
	assert.Nil(t, rayContainer.ReadinessProbe.Exec)
	assert.Equal(t, utils.RayNodeHealthPath, rayContainer.LivenessProbe.HTTPGet.Path)
	assert.Equal(t, int32(utils.DefaultDashboardAgentListenPort), rayContainer.LivenessProbe.HTTPGet.Port.IntVal)

	// Worker nodes also should have only http probes.
	rayContainer.LivenessProbe = nil
	rayContainer.ReadinessProbe = nil
	initLivenessAndReadinessProbe(rayContainer, rayv1.WorkerNode, utils.RayClusterCRD, rayStartParams, "2.53.0")
	assert.Nil(t, rayContainer.LivenessProbe.Exec)
	assert.Nil(t, rayContainer.ReadinessProbe.Exec)
	assert.NotNil(t, rayContainer.LivenessProbe.HTTPGet)
	assert.NotNil(t, rayContainer.ReadinessProbe.HTTPGet)

	// Ray Serve workers still use exec probes for readiness to check the proxy actor.
	rayContainer.LivenessProbe = nil
	rayContainer.ReadinessProbe = nil
	initLivenessAndReadinessProbe(rayContainer, rayv1.WorkerNode, utils.RayServiceCRD, rayStartParams, "2.53.0")
	assert.NotNil(t, rayContainer.LivenessProbe.HTTPGet)
	assert.Nil(t, rayContainer.LivenessProbe.Exec)
	assert.Nil(t, rayContainer.ReadinessProbe.HTTPGet)
	assert.NotNil(t, rayContainer.ReadinessProbe.Exec)
	assert.Contains(t, strings.Join(rayContainer.ReadinessProbe.Exec.Command, " "), utils.RayServeProxyHealthPath)

	// Versions parsed below 2.53 must use exec probes.
	rayContainer.LivenessProbe = nil
	rayContainer.ReadinessProbe = nil
	initLivenessAndReadinessProbe(rayContainer, rayv1.HeadNode, utils.RayClusterCRD, rayStartParams, "2.52.0")
	assert.NotNil(t, rayContainer.LivenessProbe.Exec)
	assert.NotNil(t, rayContainer.ReadinessProbe.Exec)
}

func TestGenerateRayStartCommand(t *testing.T) {
	tests := []struct {
		rayStartParams map[string]string
		name           string
		expected       string
		nodeType       rayv1.RayNodeType
		resource       corev1.ResourceRequirements
	}{
		{
			name:           "WorkerNode with GPU",
			nodeType:       rayv1.WorkerNode,
			rayStartParams: map[string]string{},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"nvidia.com/gpu": resource.MustParse("1"),
				},
			},
			expected: "ray start  --num-gpus=1 ",
		},
		{
			name:           "WorkerNode with MIG GPU",
			nodeType:       rayv1.WorkerNode,
			rayStartParams: map[string]string{},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"nvidia.com/mig-2g.32gb": resource.MustParse("1"),
				},
			},
			expected: "ray start  --num-gpus=1 ",
		},
		{
			name:           "WorkerNode with TPU",
			nodeType:       rayv1.WorkerNode,
			rayStartParams: map[string]string{},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"google.com/tpu": resource.MustParse("4"),
				},
			},
			expected: `ray start  --resources='{"TPU":4}' `,
		},
		{
			name:           "HeadNode with Neuron Cores",
			nodeType:       rayv1.HeadNode,
			rayStartParams: map[string]string{},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"aws.amazon.com/neuroncore": resource.MustParse("4"),
				},
			},
			expected: `ray start --head  --resources='{"neuron_cores":4}' `,
		},
		{
			name:           "HeadNode with multiple accelerators",
			nodeType:       rayv1.HeadNode,
			rayStartParams: map[string]string{},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"aws.amazon.com/neuroncore": resource.MustParse("4"),
					"nvidia.com/gpu":            resource.MustParse("1"),
				},
			},
			expected: `ray start --head  --num-gpus=1  --resources='{"neuron_cores":4}' `,
		},
		{
			name:           "HeadNode with multiple custom accelerators",
			nodeType:       rayv1.HeadNode,
			rayStartParams: map[string]string{},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"google.com/tpu":            resource.MustParse("8"),
					"aws.amazon.com/neuroncore": resource.MustParse("4"),
					"nvidia.com/gpu":            resource.MustParse("1"),
				},
			},
			expected: `ray start --head  --num-gpus=1  --resources='{"neuron_cores":4}' `,
		},
		{
			name:     "HeadNode with existing resources",
			nodeType: rayv1.HeadNode,
			rayStartParams: map[string]string{
				"resources": `"{"custom_resource":2}"`,
			},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"aws.amazon.com/neuroncore": resource.MustParse("4"),
				},
			},
			expected: `ray start --head  --resources='{"custom_resource":2,"neuron_cores":4}' `,
		},
		{
			name:     "HeadNode with existing neuron_cores resources",
			nodeType: rayv1.HeadNode,
			rayStartParams: map[string]string{
				"resources": `'{"custom_resource":2,"neuron_cores":3}'`,
			},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"aws.amazon.com/neuroncore": resource.MustParse("4"),
				},
			},
			expected: `ray start --head  --resources='{"custom_resource":2,"neuron_cores":3}' `,
		},
		{
			name:     "HeadNode with existing TPU resources",
			nodeType: rayv1.HeadNode,
			rayStartParams: map[string]string{
				"resources": `'{"custom_resource":2,"TPU":4}'`,
			},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"google.com/tpu": resource.MustParse("8"),
				},
			},
			expected: `ray start --head  --resources='{"custom_resource":2,"TPU":4}' `,
		},
		{
			name:     "HeadNode with invalid resources string",
			nodeType: rayv1.HeadNode,
			rayStartParams: map[string]string{
				"resources": "{",
			},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"aws.amazon.com/neuroncore": resource.MustParse("4"),
				},
			},
			expected: "ray start --head  --resources={ ",
		},
		{
			name:           "Invalid node type",
			nodeType:       "InvalidType",
			rayStartParams: map[string]string{},
			resource:       corev1.ResourceRequirements{},
			expected:       "",
		},
		{
			name:     "HeadNode with include-log-monitor=false",
			nodeType: rayv1.HeadNode,
			rayStartParams: map[string]string{
				"include-log-monitor": "false",
			},
			resource: corev1.ResourceRequirements{},
			expected: "ray start --head  --include-log-monitor=false ",
		},
		{
			name:     "HeadNode with include-log-monitor=true",
			nodeType: rayv1.HeadNode,
			rayStartParams: map[string]string{
				"include-log-monitor": "true",
			},
			resource: corev1.ResourceRequirements{},
			expected: "ray start --head  --include-log-monitor=true ",
		},
		{
			name:           "WorkerNode with Ascend910B NPU",
			nodeType:       rayv1.WorkerNode,
			rayStartParams: map[string]string{},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"huawei.com/Ascend910B": resource.MustParse("2"),
				},
			},
			expected: `ray start  --resources='{"NPU":2}' `,
		},
		{
			name:           "WorkerNode with Ascend NPU (huawei.com/npu)",
			nodeType:       rayv1.WorkerNode,
			rayStartParams: map[string]string{},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"huawei.com/npu": resource.MustParse("2"),
				},
			},
			expected: `ray start  --resources='{"NPU":2}' `,
		},
		{
			name:           "HeadNode with Ascend910B and GPU",
			nodeType:       rayv1.HeadNode,
			rayStartParams: map[string]string{},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"huawei.com/Ascend910B": resource.MustParse("2"),
					"nvidia.com/gpu":        resource.MustParse("1"),
				},
			},
			expected: `ray start --head  --num-gpus=1  --resources='{"NPU":2}' `,
		},
		{
			name:     "HeadNode with existing resources and Ascend910B",
			nodeType: rayv1.HeadNode,
			rayStartParams: map[string]string{
				"resources": `'{"custom_resource":2}'`,
			},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"huawei.com/Ascend910B": resource.MustParse("2"),
				},
			},
			expected: `ray start --head  --resources='{"NPU":2,"custom_resource":2}' `,
		},
		{
			name:     "HeadNode with existing NPU resources",
			nodeType: rayv1.HeadNode,
			rayStartParams: map[string]string{
				"resources": `'{"NPU":3,"custom_resource":2}'`,
			},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"huawei.com/Ascend910B": resource.MustParse("2"),
				},
			},
			expected: `ray start --head  --resources='{"NPU":3,"custom_resource":2}' `,
		},
		{
			name:           "HeadNode with multiple accelerators including Ascend910B",
			nodeType:       rayv1.HeadNode,
			rayStartParams: map[string]string{},
			resource: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					"huawei.com/Ascend910B":     resource.MustParse("2"),
					"google.com/tpu":            resource.MustParse("8"),
					"aws.amazon.com/neuroncore": resource.MustParse("4"),
					"nvidia.com/gpu":            resource.MustParse("1"),
				},
			},
			expected: `ray start --head  --num-gpus=1  --resources='{"neuron_cores":4}' `,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateRayStartCommand(context.TODO(), tt.nodeType, tt.rayStartParams, tt.resource)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSetAutoscalerV2EnvVars(t *testing.T) {
	tests := map[string]struct {
		podTemplate     *corev1.PodTemplateSpec
		expectedEnvVars []corev1.EnvVar
	}{
		"Pod without env vars should have autoscaler v2 env var set to true": {
			podTemplate: &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{},
					},
				},
			},
			expectedEnvVars: []corev1.EnvVar{
				{
					Name:  utils.RAY_ENABLE_AUTOSCALER_V2,
					Value: "true",
				},
			},
		},
		"Pod without autoscaler v2 env var should have autoscaler v2 env var set to true": {
			podTemplate: &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Env: []corev1.EnvVar{
								{
									Name:  "papal",
									Value: "conclave",
								},
							},
						},
					},
				},
			},
			expectedEnvVars: []corev1.EnvVar{
				{
					Name:  "papal",
					Value: "conclave",
				},
				{
					Name:  utils.RAY_ENABLE_AUTOSCALER_V2,
					Value: "true",
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			setAutoscalerV2EnvVars(tc.podTemplate)
			assert.Equal(t, tc.expectedEnvVars, tc.podTemplate.Spec.Containers[0].Env)
		})
	}
}

func TestSetAutoscalerV1EnvVars(t *testing.T) {
	tests := map[string]struct {
		podTemplate     *corev1.PodTemplateSpec
		expectedEnvVars []corev1.EnvVar
	}{
		"Pod without env vars should have autoscaler v2 env var set to false": {
			podTemplate: &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{},
					},
				},
			},
			expectedEnvVars: []corev1.EnvVar{
				{
					Name:  utils.RAY_ENABLE_AUTOSCALER_V2,
					Value: "false",
				},
			},
		},
		"Pod without autoscaler v2 env var should have autoscaler v2 env var set to false": {
			podTemplate: &corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Env: []corev1.EnvVar{
								{
									Name:  "papal",
									Value: "conclave",
								},
							},
						},
					},
				},
			},
			expectedEnvVars: []corev1.EnvVar{
				{
					Name:  "papal",
					Value: "conclave",
				},
				{
					Name:  utils.RAY_ENABLE_AUTOSCALER_V2,
					Value: "false",
				},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			setAutoscalerV1EnvVars(tc.podTemplate)
			assert.Equal(t, tc.expectedEnvVars, tc.podTemplate.Spec.Containers[0].Env)
		})
	}
}

func TestMergeLabels(t *testing.T) {
	tests := map[string]struct {
		templateSpecLabels map[string]string
		workerGroupLabels  map[string]string
		expectedLabels     map[string]string
	}{
		"Non-overlapping labels don't override.": {
			templateSpecLabels: map[string]string{"pod-label-key": "pod-label-value"},
			workerGroupLabels:  map[string]string{"ray/io:some-label": "ray-node-label-value"},
			expectedLabels:     map[string]string{"pod-label-key": "pod-label-value", "ray/io:some-label": "ray-node-label-value"},
		},
		"Overlapping labels override with precedence for group `Labels`.": {
			templateSpecLabels: map[string]string{"accelerator-type": "GPU", "market-type": "spot"},
			workerGroupLabels:  map[string]string{"accelerator-type": "TPU-V6E"},
			expectedLabels:     map[string]string{"accelerator-type": "TPU-V6E", "market-type": "spot"},
		},
		"Empty Pod Spec labels.": {
			templateSpecLabels: map[string]string{},
			workerGroupLabels:  map[string]string{"group-labels": "group-label-value"},
			expectedLabels:     map[string]string{"group-labels": "group-label-value"},
		},
		"Empty `Labels` field for group.": {
			templateSpecLabels: map[string]string{"pod-label": "pod-label-value"},
			workerGroupLabels:  map[string]string{},
			expectedLabels:     map[string]string{"pod-label": "pod-label-value"},
		},
		"Both `Labels` and labels in Pod spec nil.": {
			templateSpecLabels: nil,
			workerGroupLabels:  nil,
			expectedLabels:     map[string]string{},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			merged := mergeLabels(tc.templateSpecLabels, tc.workerGroupLabels)
			assert.Equal(t, tc.expectedLabels, merged)
		})
	}
}

func TestUpdateRayStartParamsLabels(t *testing.T) {
	tests := map[string]struct {
		initialRayStartParams  map[string]string
		groupLabels            map[string]string
		expectedRayStartParams map[string]string
	}{
		"Set labels in group `Labels` to `--labels` empty rayStartParams.": {
			initialRayStartParams: map[string]string{},
			groupLabels: map[string]string{
				"topology.kubernetes.io/zone": "us-central2",
				"ray.io/node-group":           "worker-group-1",
				"cloud.google.com/gke-spot":   "true",
			},
			// The output string is sorted alphabetically by key.
			expectedRayStartParams: map[string]string{
				"labels": "cloud.google.com/gke-spot=true,ray.io/node-group=worker-group-1,topology.kubernetes.io/zone=us-central2",
			},
		},
		" group `Labels overwrites an existing '--labels' parameter in rayStartParams": {
			initialRayStartParams: map[string]string{
				"labels":    "old=label,to-be=replaced",
				"resources": "some-resources", // should be retained
			},
			groupLabels: map[string]string{
				"new": "label",
			},
			expectedRayStartParams: map[string]string{
				"labels":    "new=label",
				"resources": "some-resources",
			},
		},
		"No-op when group `Labels` is nil": {
			initialRayStartParams:  map[string]string{"labels": "some=labels"},
			groupLabels:            nil,
			expectedRayStartParams: map[string]string{"labels": "some=labels"},
		},
		"No-op when group `Labels` is empty": {
			initialRayStartParams:  map[string]string{"labels": "some=labels"},
			groupLabels:            map[string]string{},
			expectedRayStartParams: map[string]string{"labels": "some=labels"},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// copy of rayStartParams for test
			rayStartParams := make(map[string]string)
			maps.Copy(rayStartParams, tc.initialRayStartParams)

			updateRayStartParamsLabels(rayStartParams, tc.groupLabels)

			assert.Equal(t, tc.expectedRayStartParams, rayStartParams)
		})
	}
}

func TestUpdateRayStartParamsResources(t *testing.T) {
	ctx := context.Background()

	tests := map[string]struct {
		initialRayStartParams  map[string]string
		groupResources         map[string]string
		expectedRayStartParams map[string]string
		expectedK8sResources   corev1.ResourceList
	}{
		"No-op when group `Resources` is nil or empty": {
			initialRayStartParams:  map[string]string{"existing": "true"},
			groupResources:         nil,
			expectedRayStartParams: map[string]string{"existing": "true"},
		},
		"Basic CPU and Memory set in `Resources` override rayStartParams": {
			initialRayStartParams: map[string]string{},
			groupResources: map[string]string{
				string(corev1.ResourceCPU):    "2",
				string(corev1.ResourceMemory): "4Gi",
			},
			expectedRayStartParams: map[string]string{
				"num-cpus": "2",
				"memory":   "4294967296", // 4Gi in bytes
			},
		},
		"Uppercase CPU and GPU resource names set in `Resources` override rayStartParams": {
			initialRayStartParams: map[string]string{},
			groupResources: map[string]string{
				"CPU": "2",
				"GPU": "4",
			},
			expectedRayStartParams: map[string]string{
				"num-cpus": "2",
				"num-gpus": "4",
			},
		},
		"GPU and custom TPU resource set in `Resources` override rayStartParams": {
			initialRayStartParams: map[string]string{},
			groupResources: map[string]string{
				"nvidia.com/gpu": "1",
				"TPU":            "4",
			},
			expectedRayStartParams: map[string]string{
				"num-gpus":  "1",
				"resources": "'{\"TPU\":4}'",
			},
		},
		"Top-level `Resources` should override existing resource params": {
			initialRayStartParams: map[string]string{
				"num-cpus":  "1",
				"memory":    "1000",
				"resources": "'{\"Custom-Resource\": 10}'",
			},
			groupResources: map[string]string{
				string(corev1.ResourceCPU): "4",
				"Custom-Resource":          "5",
			},
			expectedRayStartParams: map[string]string{
				"num-cpus":  "4",
				"memory":    "1000", // preserved
				"resources": "'{\"Custom-Resource\":5}'",
			},
		},
		"Ascend910B NPU resource set in `Resources`": {
			initialRayStartParams: map[string]string{},
			groupResources: map[string]string{
				"huawei.com/Ascend910B": "2",
			},
			expectedRayStartParams: map[string]string{
				"resources": "'{\"NPU\":2}'",
			},
		},
		"Ascend910c NPU resource set in `Resources`": {
			initialRayStartParams: map[string]string{},
			groupResources: map[string]string{
				"huawei.com/Ascend910c": "4",
			},
			expectedRayStartParams: map[string]string{
				"resources": "'{\"NPU\":4}'",
			},
		},
		"GPU and Ascend910B NPU resource set in `Resources`": {
			initialRayStartParams: map[string]string{},
			groupResources: map[string]string{
				"nvidia.com/gpu":        "1",
				"huawei.com/Ascend910B": "2",
			},
			expectedRayStartParams: map[string]string{
				"num-gpus":  "1",
				"resources": "'{\"NPU\":2}'",
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rayStartParams := make(map[string]string)
			maps.Copy(rayStartParams, tc.initialRayStartParams)

			updateRayStartParamsResources(ctx, rayStartParams, tc.groupResources)

			// Verify rayStartParams are updated based on the top-level Resources values.
			assert.Equal(t, tc.expectedRayStartParams, rayStartParams)
		})
	}
}

func TestConfigureTLS_Disabled(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	cluster := instance.DeepCopy()
	// TLSOptions is nil by default => TLS disabled.
	ctx := context.Background()

	podName := "test-head"
	podTemplate := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")

	// No TLS volume should be added.
	for _, vol := range podTemplate.Spec.Volumes {
		assert.NotEqual(t, utils.RayTLSVolumeName, vol.Name)
	}
	// No TLS env vars should be added.
	rayContainer := podTemplate.Spec.Containers[utils.RayContainerIndex]
	assert.Nil(t, getEnvVar(rayContainer, utils.RAY_USE_TLS))
}

func TestConfigureTLS_AutoGenerate_HeadPod(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	cluster := instance.DeepCopy()
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
	ctx := context.Background()

	podName := "test-head"
	podTemplate := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, podName, "6379")

	// Auto-generate mode mounts the cert-manager head secret.
	var tlsVolume *corev1.Volume
	for i := range podTemplate.Spec.Volumes {
		if podTemplate.Spec.Volumes[i].Name == utils.RayTLSVolumeName {
			tlsVolume = &podTemplate.Spec.Volumes[i]
			break
		}
	}
	require.NotNil(t, tlsVolume, "TLS volume should be added")
	require.NotNil(t, tlsVolume.Secret, "auto-generate mode should use Secret volume")
	expectedSecret := utils.GetTLSSecretName(cluster.Name, rayv1.HeadNode)
	assert.Equal(t, expectedSecret, tlsVolume.Secret.SecretName)

	// Verify TLS env vars on Ray container.
	rayContainer := podTemplate.Spec.Containers[utils.RayContainerIndex]
	checkContainerEnv(t, rayContainer, utils.RAY_USE_TLS, "1")
	checkContainerEnv(t, rayContainer, utils.RAY_TLS_SERVER_CERT, utils.RayTLSCertMountPath+"/tls.crt")
	checkContainerEnv(t, rayContainer, utils.RAY_TLS_SERVER_KEY, utils.RayTLSCertMountPath+"/tls.key")
	checkContainerEnv(t, rayContainer, utils.RAY_TLS_CA_CERT, utils.RayTLSCertMountPath+"/ca.crt")

	// Verify TLS volume mount on Ray container.
	var tlsMount *corev1.VolumeMount
	for i := range rayContainer.VolumeMounts {
		if rayContainer.VolumeMounts[i].Name == utils.RayTLSVolumeName {
			tlsMount = &rayContainer.VolumeMounts[i]
			break
		}
	}
	require.NotNil(t, tlsMount, "TLS volume mount should be added to Ray container")
	assert.Equal(t, utils.RayTLSCertMountPath, tlsMount.MountPath)
	assert.True(t, tlsMount.ReadOnly)

	// wait-for-tls-ip-san must be the first init container on head pods (auto-generate only).
	require.NotEmpty(t, podTemplate.Spec.InitContainers, "head pod should have init containers when TLS is enabled")
	assert.Equal(t, "wait-for-tls-ip-san", podTemplate.Spec.InitContainers[0].Name,
		"wait-for-tls-ip-san must be the first init container")
	waitInit := podTemplate.Spec.InitContainers[0]
	// Must have access to the TLS cert.
	hasTLSMount := false
	for _, vm := range waitInit.VolumeMounts {
		if vm.Name == utils.RayTLSVolumeName {
			hasTLSMount = true
			break
		}
	}
	assert.True(t, hasTLSMount, "wait-for-tls-ip-san should mount the TLS volume")
	// Must receive POD_IP from the downward API.
	hasPodIPEnv := false
	for _, e := range waitInit.Env {
		if e.Name == "POD_IP" && e.ValueFrom != nil &&
			e.ValueFrom.FieldRef != nil && e.ValueFrom.FieldRef.FieldPath == "status.podIP" {
			hasPodIPEnv = true
			break
		}
	}
	assert.True(t, hasPodIPEnv, "wait-for-tls-ip-san should receive POD_IP via downward API")
}

func TestConfigureTLS_AutoGenerate_WorkerPod(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	cluster := instance.DeepCopy()
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
	ctx := context.Background()

	worker := cluster.Spec.WorkerGroupSpecs[0]
	fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
	podTemplate := DefaultWorkerPodTemplate(ctx, *cluster, worker, "test-worker", fqdnRayIP, "6379", "", 0, 0)

	// Auto-generate mode mounts the cert-manager worker secret.
	var tlsVolume *corev1.Volume
	for i := range podTemplate.Spec.Volumes {
		if podTemplate.Spec.Volumes[i].Name == utils.RayTLSVolumeName {
			tlsVolume = &podTemplate.Spec.Volumes[i]
			break
		}
	}
	require.NotNil(t, tlsVolume, "TLS volume should be added")
	require.NotNil(t, tlsVolume.Secret, "auto-generate mode should use Secret volume")
	expectedSecret := utils.GetTLSSecretName(cluster.Name, rayv1.WorkerNode)
	assert.Equal(t, expectedSecret, tlsVolume.Secret.SecretName)

	// Verify TLS env vars on Ray container.
	rayContainer := podTemplate.Spec.Containers[utils.RayContainerIndex]
	checkContainerEnv(t, rayContainer, utils.RAY_USE_TLS, "1")
	checkContainerEnv(t, rayContainer, utils.RAY_TLS_SERVER_CERT, utils.RayTLSCertMountPath+"/tls.crt")
	checkContainerEnv(t, rayContainer, utils.RAY_TLS_SERVER_KEY, utils.RayTLSCertMountPath+"/tls.key")
	checkContainerEnv(t, rayContainer, utils.RAY_TLS_CA_CERT, utils.RayTLSCertMountPath+"/ca.crt")
	// Verify TLS volume mount on Ray container.
	var tlsMount *corev1.VolumeMount
	for i := range rayContainer.VolumeMounts {
		if rayContainer.VolumeMounts[i].Name == utils.RayTLSVolumeName {
			tlsMount = &rayContainer.VolumeMounts[i]
			break
		}
	}
	require.NotNil(t, tlsMount, "TLS volume mount should be added to Ray container")
	assert.Equal(t, utils.RayTLSCertMountPath, tlsMount.MountPath)
	assert.True(t, tlsMount.ReadOnly)

	// wait-for-tls-ip-san must be the first init container on worker pods. GCS connects back
	// to each worker's raylet using the worker's pod IP; if the cert doesn't yet list that IP
	// the TLS handshake fails and GCS marks the worker dead, causing the RayJob to fail.
	require.NotEmpty(t, podTemplate.Spec.InitContainers, "worker pod should have init containers")
	assert.Equal(t, "wait-for-tls-ip-san", podTemplate.Spec.InitContainers[0].Name,
		"wait-for-tls-ip-san must be the first init container on worker pods")

	// wait-gcs-ready should exist and have TLS config.
	var gcsReadyContainer *corev1.Container
	for i := range podTemplate.Spec.InitContainers {
		if podTemplate.Spec.InitContainers[i].Name == "wait-gcs-ready" {
			gcsReadyContainer = &podTemplate.Spec.InitContainers[i]
			break
		}
	}
	require.NotNil(t, gcsReadyContainer, "worker pod should have wait-gcs-ready init container")
	assert.NotNil(t, getEnvVar(*gcsReadyContainer, utils.RAY_USE_TLS), "wait-gcs-ready should have RAY_USE_TLS")
}

func TestConfigureTLS_AutoscalerContainer(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	cluster := instance.DeepCopy()
	cluster.Spec.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
	cluster.Spec.EnableInTreeAutoscaling = new(true)
	ctx := context.Background()

	podTemplate := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, "test-head", "6379")

	// Find the autoscaler container.
	var autoscalerContainer *corev1.Container
	for i, c := range podTemplate.Spec.Containers {
		if c.Name == "autoscaler" {
			autoscalerContainer = &podTemplate.Spec.Containers[i]
			break
		}
	}
	require.NotNil(t, autoscalerContainer, "autoscaler container should exist when autoscaling is enabled")

	// Verify TLS env vars on autoscaler container.
	env := getEnvVar(*autoscalerContainer, utils.RAY_USE_TLS)
	assert.NotNil(t, env, "autoscaler container should have RAY_USE_TLS")
	checkContainerEnv(t, *autoscalerContainer, utils.RAY_TLS_SERVER_CERT, utils.RayTLSCertMountPath+"/tls.crt")
	checkContainerEnv(t, *autoscalerContainer, utils.RAY_TLS_SERVER_KEY, utils.RayTLSCertMountPath+"/tls.key")
	checkContainerEnv(t, *autoscalerContainer, utils.RAY_TLS_CA_CERT, utils.RayTLSCertMountPath+"/ca.crt")

	// Verify TLS volume mount on autoscaler container.
	var tlsMount *corev1.VolumeMount
	for i := range autoscalerContainer.VolumeMounts {
		if autoscalerContainer.VolumeMounts[i].Name == utils.RayTLSVolumeName {
			tlsMount = &autoscalerContainer.VolumeMounts[i]
			break
		}
	}
	assert.NotNil(t, tlsMount, "autoscaler container should have TLS volume mount")
}

func TestSetContainerTLSConfig(t *testing.T) {
	container := &corev1.Container{Name: "test"}
	SetContainerTLSConfig(container)

	assert.Len(t, container.Env, 4, "should add 4 TLS env vars")
	assert.Equal(t, utils.RAY_USE_TLS, container.Env[0].Name)
	assert.Equal(t, "1", container.Env[0].Value)
	assert.Equal(t, utils.RAY_TLS_SERVER_CERT, container.Env[1].Name)
	assert.Equal(t, utils.RAY_TLS_SERVER_KEY, container.Env[2].Name)
	assert.Equal(t, utils.RAY_TLS_CA_CERT, container.Env[3].Name)

	require.Len(t, container.VolumeMounts, 1, "should add 1 TLS volume mount")
	assert.Equal(t, utils.RayTLSVolumeName, container.VolumeMounts[0].Name)
	assert.Equal(t, utils.RayTLSCertMountPath, container.VolumeMounts[0].MountPath)
	assert.True(t, container.VolumeMounts[0].ReadOnly)
}

func TestBuildCollectorContainerAndPodInjection(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterHistoryServer, true)
	ctx := context.Background()
	cluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
			Labels: map[string]string{
				utils.RayOriginatedFromCRDLabelKey:    "RayJob",
				utils.RayOriginatedFromCRNameLabelKey: "test-rayjob",
			},
		},
		Spec: rayv1.RayClusterSpec{
			HistoryServerOptions: &rayv1.HistoryServerOptions{
				CollectorOptions: &rayv1.CollectorOptions{
					Image: new("quay.io/kuberay/collector:latest"),
					Env: []corev1.EnvVar{
						{Name: "STORAGE_BACKEND", Value: "GCS"},
						{Name: "GCS_BUCKET", Value: "my-bucket"},
					},
				},
			},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{},
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "ray-head", Image: "rayproject/ray:latest"},
						},
					},
				},
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:      "worker-group",
					RayStartParams: map[string]string{},
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: "ray-worker", Image: "rayproject/ray:latest"},
							},
						},
					},
				},
			},
		},
	}

	fqdnRayIP := "test-cluster-head-svc.default.svc.cluster.local"
	container := BuildCollectorContainer(cluster.Spec.HistoryServerOptions.CollectorOptions, rayv1.HeadNode, cluster.Name, cluster.Namespace, fqdnRayIP, cluster.Labels)
	assert.Equal(t, utils.CollectorContainerName, container.Name)
	assert.Equal(t, "quay.io/kuberay/collector:latest", container.Image)
	eventsPortEnv, ok := utils.EnvVarByName(utils.EVENTS_PORT, container.Env)
	assert.True(t, ok)
	assert.Equal(t, "8084", eventsPortEnv.Value)
	assert.True(t, utils.EnvVarExists("POD_IP", container.Env))
	assert.True(t, utils.EnvVarExists("FQ_RAY_IP", container.Env))
	assert.True(t, utils.EnvVarExists("RAY_CLUSTER_NAME", container.Env))
	assert.True(t, utils.EnvVarExists("RAY_CLUSTER_NAMESPACE", container.Env))
	assert.True(t, utils.EnvVarExists("RAY_ROLE", container.Env))
	ownerKindEnv, ok := utils.EnvVarByName("OWNER_KIND", container.Env)
	assert.True(t, ok)
	assert.Equal(t, "RayJob", ownerKindEnv.Value)
	ownerNameEnv, ok := utils.EnvVarByName("OWNER_NAME", container.Env)
	assert.True(t, ok)
	assert.Equal(t, "test-rayjob", ownerNameEnv.Value)
	assert.True(t, utils.EnvVarExists("STORAGE_BACKEND", container.Env))
	assert.True(t, utils.EnvVarExists("GCS_BUCKET", container.Env))
	dashboardAddrEnv, ok := utils.EnvVarByName(utils.RAY_DASHBOARD_ADDRESS, container.Env)
	assert.True(t, ok)
	assert.Equal(t, "http://localhost:8265", dashboardAddrEnv.Value)

	// Test DefaultHeadPodTemplate injection and BuildPod volume mounting
	headPodTemplate := DefaultHeadPodTemplate(ctx, *cluster, cluster.Spec.HeadGroupSpec, "pod-head", "6379")
	assert.Len(t, headPodTemplate.Spec.Containers, 2)
	assert.Equal(t, utils.CollectorContainerName, headPodTemplate.Spec.Containers[1].Name)

	headPod := BuildPod(ctx, headPodTemplate, rayv1.HeadNode, cluster.Spec.HeadGroupSpec.RayStartParams, "6379", false, utils.RayJobCRD, fqdnRayIP, nil, "")
	assert.True(t, utils.VolumeExists(RayLogVolumeName, headPod.Spec.Volumes))
	assert.True(t, utils.VolumeMountExists(RayLogVolumeName, headPod.Spec.Containers[utils.RayContainerIndex].VolumeMounts))
	assert.True(t, utils.VolumeMountExists(RayLogVolumeName, headPod.Spec.Containers[1].VolumeMounts))
	assert.True(t, utils.EnvVarExists(utils.RAY_DASHBOARD_ADDRESS, headPod.Spec.Containers[1].Env))
	eventEnableEnv, ok := utils.EnvVarByName(utils.RAY_ENABLE_RAY_EVENT, headPod.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, ok)
	assert.Equal(t, "true", eventEnableEnv.Value)
	coreWorkerEventEnv, ok := utils.EnvVarByName(utils.RAY_ENABLE_CORE_WORKER_RAY_EVENT_TO_AGGREGATOR, headPod.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, ok)
	assert.Equal(t, "true", coreWorkerEventEnv.Value)
	eventExportEnv, ok := utils.EnvVarByName(utils.RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR, headPod.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, ok)
	assert.Equal(t, "http://localhost:8084/v1/events", eventExportEnv.Value)
	eventTypesEnv, ok := utils.EnvVarByName(utils.RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES, headPod.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, ok)
	assert.Equal(t, utils.DEFAULT_RAY_EXPOSABLE_EVENT_TYPES, eventTypesEnv.Value)
	eventTypesV2Env, ok := utils.EnvVarByName(utils.RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_HTTP_ENDPOINT_EXPOSABLE_EVENT_TYPES, headPod.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, ok)
	assert.Equal(t, utils.DEFAULT_RAY_EXPOSABLE_EVENT_TYPES, eventTypesV2Env.Value)

	// Test DefaultWorkerPodTemplate injection and BuildPod volume mounting
	workerPodTemplate := DefaultWorkerPodTemplate(ctx, *cluster, cluster.Spec.WorkerGroupSpecs[0], "pod-worker", fqdnRayIP, "6379", "group-1", 0, 0)
	assert.Len(t, workerPodTemplate.Spec.Containers, 2)
	assert.Equal(t, utils.CollectorContainerName, workerPodTemplate.Spec.Containers[1].Name)

	workerPod := BuildPod(ctx, workerPodTemplate, rayv1.WorkerNode, cluster.Spec.WorkerGroupSpecs[0].RayStartParams, "6379", false, utils.RayJobCRD, fqdnRayIP, nil, "")
	assert.True(t, utils.VolumeExists(RayLogVolumeName, workerPod.Spec.Volumes))
	assert.True(t, utils.VolumeMountExists(RayLogVolumeName, workerPod.Spec.Containers[utils.RayContainerIndex].VolumeMounts))
	assert.True(t, utils.VolumeMountExists(RayLogVolumeName, workerPod.Spec.Containers[1].VolumeMounts))
	assert.False(t, utils.EnvVarExists(utils.RAY_DASHBOARD_ADDRESS, workerPod.Spec.Containers[1].Env))
	workerEventEnableEnv, ok := utils.EnvVarByName(utils.RAY_ENABLE_RAY_EVENT, workerPod.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, ok)
	assert.Equal(t, "true", workerEventEnableEnv.Value)
	workerCoreWorkerEventEnv, ok := utils.EnvVarByName(utils.RAY_ENABLE_CORE_WORKER_RAY_EVENT_TO_AGGREGATOR, workerPod.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, ok)
	assert.Equal(t, "true", workerCoreWorkerEventEnv.Value)
	workerEventExportEnv, ok := utils.EnvVarByName(utils.RAY_DASHBOARD_AGGREGATOR_AGENT_EVENTS_EXPORT_ADDR, workerPod.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, ok)
	assert.Equal(t, "http://localhost:8084/v1/events", workerEventExportEnv.Value)
	workerEventTypesEnv, ok := utils.EnvVarByName(utils.RAY_DASHBOARD_AGGREGATOR_AGENT_EXPOSABLE_EVENT_TYPES, workerPod.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, ok)
	assert.Equal(t, utils.DEFAULT_RAY_EXPOSABLE_EVENT_TYPES, workerEventTypesEnv.Value)
	workerEventTypesV2Env, ok := utils.EnvVarByName(utils.RAY_DASHBOARD_AGGREGATOR_AGENT_PUBLISHER_HTTP_ENDPOINT_EXPOSABLE_EVENT_TYPES, workerPod.Spec.Containers[utils.RayContainerIndex].Env)
	assert.True(t, ok)
	assert.Equal(t, utils.DEFAULT_RAY_EXPOSABLE_EVENT_TYPES, workerEventTypesV2Env.Value)
}

func TestIsNPUResourceKey(t *testing.T) {
	tests := []struct {
		name        string
		resourceKey string
		expected    bool
	}{
		{
			name:        "huawei.com/Ascend910B",
			resourceKey: "huawei.com/Ascend910B",
			expected:    true,
		},
		{
			name:        "huawei.com/Ascend910B-power",
			resourceKey: "huawei.com/Ascend910B-power",
			expected:    true,
		},
		{
			name:        "huawei.com/npu",
			resourceKey: "huawei.com/npu",
			expected:    true,
		},
		{
			name:        "huawei.com/NPU",
			resourceKey: "huawei.com/NPU",
			expected:    true,
		},
		{
			name:        "huawei.com/ascend-memory excluded",
			resourceKey: "huawei.com/ascend-memory",
			expected:    false,
		},
		{
			name:        "huawei.com/ascend-core excluded",
			resourceKey: "huawei.com/ascend-core",
			expected:    false,
		},
		{
			name:        "huawei.com/Ascend910B-memory excluded",
			resourceKey: "huawei.com/Ascend910B-memory",
			expected:    false,
		},
		{
			name:        "huawei.com/Ascend910B-core excluded",
			resourceKey: "huawei.com/Ascend910B-core",
			expected:    false,
		},
		{
			name:        "nvidia gpu not NPU",
			resourceKey: "nvidia.com/gpu",
			expected:    false,
		},
		{
			name:        "cpu not NPU",
			resourceKey: "cpu",
			expected:    false,
		},
		{
			name:        "memory not NPU",
			resourceKey: "memory",
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isNPUResourceKey(tt.resourceKey)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetCustomAcceleratorRayResourceName(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		expected string
	}{
		{
			name:     "nvidia neuron core direct match",
			key:      "aws.amazon.com/neuroncore",
			expected: "neuron_cores",
		},
		{
			name:     "google TPU direct match",
			key:      "google.com/tpu",
			expected: "TPU",
		},
		{
			name:     "huawei Ascend910B NPU",
			key:      "huawei.com/Ascend910B",
			expected: "NPU",
		},
		{
			name:     "huawei.com/npu NPU",
			key:      "huawei.com/npu",
			expected: "NPU",
		},
		{
			name:     "huawei ascend-memory returns empty",
			key:      "huawei.com/ascend-memory",
			expected: "",
		},
		{
			name:     "unknown resource returns empty",
			key:      "unknown.com/accelerator",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getCustomAcceleratorRayResourceName(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSetDefaultCollectorImage(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterHistoryServer, true)

	defaultCollectorImage := "quay.io/kuberay/collector:nightly"

	cluster := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			HistoryServerOptions: &rayv1.HistoryServerOptions{
				CollectorOptions: &rayv1.CollectorOptions{},
			},
		},
	}

	tests := []struct {
		name           string
		collectorImage string
		expectedImage  string
	}{
		{
			name:           "collector image is inherited from the operator when not set",
			collectorImage: "",
			expectedImage:  defaultCollectorImage,
		},
		{
			name:           "collector image set in the RayCluster is not overridden",
			collectorImage: "my-registry.io/kuberay/collector:v1.7.0",
			expectedImage:  "my-registry.io/kuberay/collector:v1.7.0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			podTemplate := corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "ray-head", Image: "rayproject/ray:latest"},
						{Name: utils.CollectorContainerName, Image: tc.collectorImage},
					},
				},
			}

			SetDefaultCollectorImage(cluster, &podTemplate, defaultCollectorImage)

			assert.Equal(t, "rayproject/ray:latest", podTemplate.Spec.Containers[0].Image)
			assert.Equal(t, tc.expectedImage, podTemplate.Spec.Containers[1].Image)
		})
	}

	t.Run("no-op when the collector container is not injected", func(t *testing.T) {
		podTemplate := corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: "ray-head", Image: "rayproject/ray:latest"},
				},
			},
		}

		SetDefaultCollectorImage(cluster, &podTemplate, defaultCollectorImage)

		assert.Len(t, podTemplate.Spec.Containers, 1)
		assert.Equal(t, "rayproject/ray:latest", podTemplate.Spec.Containers[0].Image)
	})

	t.Run("no-op when the RayCluster does not set historyServerOptions.collectorOptions", func(t *testing.T) {
		// A collector container the user added manually is not managed by the operator,
		// so its empty image is left as is.
		podTemplate := corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{Name: utils.CollectorContainerName, Image: ""},
				},
			},
		}

		SetDefaultCollectorImage(&rayv1.RayCluster{}, &podTemplate, defaultCollectorImage)

		assert.Empty(t, podTemplate.Spec.Containers[0].Image)
	})
}

// TestBuildPod_GeneratedRayStartCmdIsArgsSubstring pins the contract the pods CREATE webhook relies on: the
// KUBERAY_GEN_RAY_START_CMD value appears exactly once, verbatim, in the Ray container's single args element
func TestBuildPod_GeneratedRayStartCmdIsArgsSubstring(t *testing.T) {
	ctx := context.Background()
	for _, userCommand := range [][]string{nil, {"echo hello"}} {
		cluster := instance.DeepCopy()
		worker := cluster.Spec.WorkerGroupSpecs[0]
		worker.Template.Spec.Containers[utils.RayContainerIndex].Command = userCommand
		podName := cluster.Name + utils.DashSymbol + string(rayv1.WorkerNode) + utils.DashSymbol + worker.GroupName + utils.DashSymbol + utils.FormatInt32(0)
		fqdnRayIP := utils.GenerateFQDNServiceName(ctx, *cluster, cluster.Namespace)
		podTemplateSpec := DefaultWorkerPodTemplate(ctx, *cluster, worker, podName, fqdnRayIP, "6379", "", 0, 0)
		pod := BuildPod(ctx, podTemplateSpec, rayv1.WorkerNode, worker.RayStartParams, "6379", false, utils.GetCRDType(""), fqdnRayIP, nil, "")

		container := pod.Spec.Containers[utils.RayContainerIndex]
		generated := getEnvVar(container, utils.KUBERAY_GEN_RAY_START_CMD)
		require.NotNil(t, generated)
		require.Len(t, container.Args, 1)
		assert.Equal(t, 1, strings.Count(container.Args[0], generated.Value), "args: %q", container.Args[0])
	}
}
