package utils

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

func TestValidateRayClusterStatus(t *testing.T) {
	tests := []struct {
		name        string
		conditions  []metav1.Condition
		expectError bool
	}{
		{
			name: "Both suspending and suspended are true",
			conditions: []metav1.Condition{
				{
					Type:   string(rayv1.RayClusterSuspending),
					Status: metav1.ConditionTrue,
				},
				{
					Type:   string(rayv1.RayClusterSuspended),
					Status: metav1.ConditionTrue,
				},
			},
			expectError: true,
		},
		{
			name: "Only suspending is true",
			conditions: []metav1.Condition{
				{
					Type:   string(rayv1.RayClusterSuspending),
					Status: metav1.ConditionTrue,
				},
				{
					Type:   string(rayv1.RayClusterSuspended),
					Status: metav1.ConditionFalse,
				},
			},
			expectError: false,
		},
		{
			name: "Only suspended is true",
			conditions: []metav1.Condition{
				{
					Type:   string(rayv1.RayClusterSuspending),
					Status: metav1.ConditionFalse,
				},
				{
					Type:   string(rayv1.RayClusterSuspended),
					Status: metav1.ConditionTrue,
				},
			},
			expectError: false,
		},
		{
			name: "Both suspending and suspended are false",
			conditions: []metav1.Condition{
				{
					Type:   string(rayv1.RayClusterSuspending),
					Status: metav1.ConditionFalse,
				},
				{
					Type:   string(rayv1.RayClusterSuspended),
					Status: metav1.ConditionFalse,
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instance := &rayv1.RayCluster{
				Status: rayv1.RayClusterStatus{
					Conditions: tt.conditions,
				},
			}
			err := ValidateRayClusterStatus(instance)
			if (err != nil) != tt.expectError {
				t.Errorf("ValidateRayClusterStatus() error = %v, wantErr %v", err, tt.expectError)
			}
		})
	}
}

func TestValidateRayClusterSpecGcsFaultToleranceOptions(t *testing.T) {
	errorMessageBothSet := fmt.Sprintf("%s annotation and GcsFaultToleranceOptions are both set. "+
		"Please use only GcsFaultToleranceOptions to configure GCS fault tolerance", RayFTEnabledAnnotationKey)
	errorMessageRedisAddressSet := fmt.Sprintf("%s is set which implicitly enables GCS fault tolerance, "+
		"but GcsFaultToleranceOptions is not set. Please set GcsFaultToleranceOptions "+
		"to enable GCS fault tolerance", RAY_REDIS_ADDRESS)
	errorMessageRedisAddressConflict := fmt.Sprintf("cannot set `%s` env var in head Pod when "+
		"GcsFaultToleranceOptions is enabled - use GcsFaultToleranceOptions.RedisAddress instead", RAY_REDIS_ADDRESS)
	errorMessageExternalStorageNamespaceConflict := fmt.Sprintf("cannot set `%s` annotation when "+
		"GcsFaultToleranceOptions is enabled - use GcsFaultToleranceOptions.ExternalStorageNamespace instead", RayExternalStorageNSAnnotationKey)

	tests := []struct {
		rayStartParams           map[string]string
		gcsFaultToleranceOptions *rayv1.GcsFaultToleranceOptions
		annotations              map[string]string
		name                     string
		errorMessage             string
		envVars                  []corev1.EnvVar
		expectError              bool
	}{
		// GcsFaultToleranceOptions and ray.io/ft-enabled should not be both set.
		{
			name: "ray.io/ft-enabled is set to false and GcsFaultToleranceOptions is set",
			annotations: map[string]string{
				RayFTEnabledAnnotationKey: "false",
			},
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{},
			expectError:              true,
			errorMessage:             errorMessageBothSet,
		},
		{
			name: "ray.io/ft-enabled is set to true and GcsFaultToleranceOptions is set",
			annotations: map[string]string{
				RayFTEnabledAnnotationKey: "true",
			},
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{},
			expectError:              true,
			errorMessage:             errorMessageBothSet,
		},
		{
			name:                     "ray.io/ft-enabled is not set and GcsFaultToleranceOptions is set",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{RedisAddress: "redis:6379"},
			expectError:              false,
		},
		{
			name:                     "ray.io/ft-enabled is not set and GcsFaultToleranceOptions is not set",
			gcsFaultToleranceOptions: nil,
			expectError:              false,
		},
		// RAY_REDIS_ADDRESS should not be set if KubeRay is not aware that GCS fault tolerance is enabled.
		{
			name: "ray.io/ft-enabled is set to false and RAY_REDIS_ADDRESS is set",
			annotations: map[string]string{
				RayFTEnabledAnnotationKey: "false",
			},
			envVars: []corev1.EnvVar{
				{
					Name:  RAY_REDIS_ADDRESS,
					Value: "redis:6379",
				},
			},
			expectError:  true,
			errorMessage: errorMessageRedisAddressSet,
		},
		{
			name: "gcsFaultToleranceOptions is set and RAY_REDIS_ADDRESS is set",
			envVars: []corev1.EnvVar{
				{
					Name:  RAY_REDIS_ADDRESS,
					Value: "redis:6379",
				},
			},
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{},
			expectError:              true,
			errorMessage:             errorMessageRedisAddressConflict,
		},
		{
			name: "FT is disabled and RAY_REDIS_ADDRESS is set",
			envVars: []corev1.EnvVar{
				{
					Name:  RAY_REDIS_ADDRESS,
					Value: "redis:6379",
				},
			},
			expectError:  true,
			errorMessage: errorMessageRedisAddressSet,
		},
		{
			name: "ray.io/ft-enabled is set to true and RAY_REDIS_ADDRESS is set",
			annotations: map[string]string{
				RayFTEnabledAnnotationKey: "true",
			},
			envVars: []corev1.EnvVar{
				{
					Name:  RAY_REDIS_ADDRESS,
					Value: "redis:6379",
				},
			},
			expectError: false,
		},
		{
			name: "gcsFaultToleranceOptions is set and ray.io/external-storage-namespace is set",
			annotations: map[string]string{
				RayExternalStorageNSAnnotationKey: "myns",
			},
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{RedisAddress: "redis:6379"},
			expectError:              true,
			errorMessage:             errorMessageExternalStorageNamespaceConflict,
		},
		{
			name:                     "redis backend rejects empty RedisAddress",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRedis},
			expectError:              true,
			errorMessage:             "GcsFaultToleranceOptions.RedisAddress must be set when backend is 'redis'",
		},
		{
			name: "redis backend rejects rocksdb-only storage field",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{
				Backend: rayv1.GcsFTBackendRedis,
				Storage: &rayv1.GcsEmbeddedStorage{Size: new(resource.MustParse("1Gi"))},
			},
			expectError:  true,
			errorMessage: "cannot set GcsFaultToleranceOptions.Storage when backend is 'redis' - it only applies to the 'rocksdb' backend",
		},
		// rocksdb backend rules.
		{
			name:                     "rocksdb backend is valid with no redis fields",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRocksDB},
			expectError:              false,
		},
		{
			name: "rocksdb backend with operator-managed storage is valid",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{
				Backend: rayv1.GcsFTBackendRocksDB,
				Storage: &rayv1.GcsEmbeddedStorage{Size: new(resource.MustParse("2Gi"))},
			},
			expectError: false,
		},
		{
			name: "rocksdb backend rejects RedisAddress",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{
				Backend:      rayv1.GcsFTBackendRocksDB,
				RedisAddress: "redis:6379",
			},
			expectError:  true,
			errorMessage: "cannot set GcsFaultToleranceOptions.RedisAddress when backend is 'rocksdb'",
		},
		{
			name: "rocksdb backend rejects ExternalStorageNamespace",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{
				Backend:                  rayv1.GcsFTBackendRocksDB,
				ExternalStorageNamespace: "ns",
			},
			expectError:  true,
			errorMessage: "cannot set GcsFaultToleranceOptions.ExternalStorageNamespace when backend is 'rocksdb'",
		},
		{
			name: "rocksdb backend rejects claimName combined with size",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{
				Backend: rayv1.GcsFTBackendRocksDB,
				Storage: &rayv1.GcsEmbeddedStorage{
					ClaimName: "my-pvc",
					Size:      new(resource.MustParse("1Gi")),
				},
			},
			expectError:  true,
			errorMessage: "GcsFaultToleranceOptions.Storage.ClaimName is mutually exclusive with size, storageClassName, and accessModes",
		},
		{
			name: "rocksdb backend rejects user-set RAY_gcs_storage env",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{
				Backend: rayv1.GcsFTBackendRocksDB,
			},
			envVars: []corev1.EnvVar{
				{Name: RAY_GCS_STORAGE, Value: "rocksdb"},
			},
			expectError: true,
			errorMessage: fmt.Sprintf("cannot set `%s` or `%s` env var in head Pod when the embedded GCS FT backend is used - these are managed by KubeRay",
				RAY_GCS_STORAGE, RAY_GCS_STORAGE_PATH),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.GCSFaultToleranceEmbeddedStorage, true)
			err := ValidateRayClusterSpec(&rayv1.RayClusterSpec{
				GcsFaultToleranceOptions: tt.gcsFaultToleranceOptions,
				HeadGroupSpec: rayv1.HeadGroupSpec{
					RayStartParams: tt.rayStartParams,
					Template:       podTemplateSpec(tt.envVars, nil),
				},
			}, tt.annotations)
			if tt.expectError {
				require.Error(t, err)
				assert.EqualError(t, err, tt.errorMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateRayClusterSpecEmbeddedGCSFeatureGate verifies the embedded RocksDB
// backend is rejected unless the GCSFaultToleranceEmbeddedStorage feature gate is
// enabled.
func TestValidateRayClusterSpecEmbeddedGCSFeatureGate(t *testing.T) {
	spec := &rayv1.RayClusterSpec{
		GcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRocksDB},
		HeadGroupSpec: rayv1.HeadGroupSpec{
			Template: podTemplateSpec(nil, nil),
		},
	}

	features.SetFeatureGateDuringTest(t, features.GCSFaultToleranceEmbeddedStorage, false)
	err := ValidateRayClusterSpec(spec, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "requires the GCSFaultToleranceEmbeddedStorage feature gate")

	features.SetFeatureGateDuringTest(t, features.GCSFaultToleranceEmbeddedStorage, true)
	require.NoError(t, ValidateRayClusterSpec(spec, nil))
}

func TestValidateGcsActivePassiveHead(t *testing.T) {
	enabled := true
	disabled := false

	tests := []struct {
		options      *rayv1.GcsFaultToleranceOptions
		name         string
		errorMessage string
		gateEnabled  bool
		expectError  bool
	}{
		{
			name:        "activePassiveHeadOptions not set",
			options:     &rayv1.GcsFaultToleranceOptions{RedisAddress: "redis:6379"},
			gateEnabled: true,
		},
		{
			name: "disabled",
			options: &rayv1.GcsFaultToleranceOptions{
				ActivePassiveHeadOptions: &rayv1.ActivePassiveHeadOptions{Enabled: &disabled},
			},
			gateEnabled: true,
		},
		{
			name: "feature gate disabled",
			options: &rayv1.GcsFaultToleranceOptions{
				RedisAddress:             "redis:6379",
				ActivePassiveHeadOptions: &rayv1.ActivePassiveHeadOptions{Enabled: &enabled},
			},
			gateEnabled:  false,
			expectError:  true,
			errorMessage: "activePassiveHeadOptions requires the GCSFaultToleranceActivePassiveHead feature gate to be enabled",
		},
		{
			name: "rocksdb backend not supported",
			options: &rayv1.GcsFaultToleranceOptions{
				Backend:                  rayv1.GcsFTBackendRocksDB,
				ActivePassiveHeadOptions: &rayv1.ActivePassiveHeadOptions{Enabled: &enabled},
			},
			gateEnabled:  true,
			expectError:  true,
			errorMessage: "activePassiveHeadOptions is only supported with the 'redis' backend",
		},
		{
			name: "valid",
			options: &rayv1.GcsFaultToleranceOptions{
				RedisAddress:             "redis:6379",
				ActivePassiveHeadOptions: &rayv1.ActivePassiveHeadOptions{Enabled: &enabled},
			},
			gateEnabled: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.GCSFaultToleranceActivePassiveHead, tt.gateEnabled)
			err := validateGcsActivePassiveHead(tt.options)
			if tt.expectError {
				require.Error(t, err)
				assert.EqualError(t, err, tt.errorMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateGcsFaultToleranceEmbeddedReservedVolume(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.GCSFaultToleranceEmbeddedStorage, true)
	// The operator reserves the "gcs-storage" volume name and the /data/gcs
	// mount path for the embedded backend. A user-supplied head volume, volume
	// mount by that name, or mount at that path must be rejected, otherwise
	// configureEmbeddedFT would append a duplicate and the head Pod would fail.
	newSpec := func(mounts []corev1.VolumeMount, volumes []corev1.Volume) *rayv1.RayClusterSpec {
		return &rayv1.RayClusterSpec{
			GcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{Backend: rayv1.GcsFTBackendRocksDB},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{VolumeMounts: mounts}},
						Volumes:    volumes,
					},
				},
			},
		}
	}

	tests := []struct {
		name        string
		spec        *rayv1.RayClusterSpec
		expectError bool
	}{
		{
			name:        "no reserved volume is valid",
			spec:        newSpec(nil, nil),
			expectError: false,
		},
		{
			name:        "reserved mount path is rejected",
			spec:        newSpec([]corev1.VolumeMount{{Name: "user-vol", MountPath: GCSStorageMountPath}}, nil),
			expectError: true,
		},
		{
			name:        "reserved volume mount name is rejected",
			spec:        newSpec([]corev1.VolumeMount{{Name: GCSStorageVolumeName, MountPath: "/somewhere/else"}}, nil),
			expectError: true,
		},
		{
			name:        "reserved volume name is rejected",
			spec:        newSpec(nil, []corev1.Volume{{Name: GCSStorageVolumeName}}),
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRayClusterSpec(tt.spec, nil)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpecRedisPassword(t *testing.T) {
	tests := []struct {
		gcsFaultToleranceOptions *rayv1.GcsFaultToleranceOptions
		name                     string
		rayStartParams           map[string]string
		envVars                  []corev1.EnvVar
		expectError              bool
	}{
		{
			name:                     "GcsFaultToleranceOptions is set and `redis-password` is also set in rayStartParams",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{},
			rayStartParams: map[string]string{
				"redis-password": "password",
			},
			expectError: true,
		},
		{
			name:                     "GcsFaultToleranceOptions is set and `REDIS_PASSWORD` env var is also set in the head Pod",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{},
			envVars: []corev1.EnvVar{
				{
					Name:  REDIS_PASSWORD,
					Value: "password",
				},
			},
			expectError: true,
		},
		{
			name: "GcsFaultToleranceOptions.RedisPassword is set",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{
				RedisAddress: "redis:6379",
				RedisPassword: &rayv1.RedisCredential{
					Value: "password",
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rayCluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					GcsFaultToleranceOptions: tt.gcsFaultToleranceOptions,
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: tt.rayStartParams,
						Template:       podTemplateSpec(tt.envVars, nil),
					},
				},
			}
			err := ValidateRayClusterSpec(&rayCluster.Spec, rayCluster.Annotations)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpecRedisUsername(t *testing.T) {
	errorMessageRedisUsername := "cannot set redis username in rayStartParams or environment variables - use GcsFaultToleranceOptions.RedisUsername instead"

	tests := []struct {
		gcsFaultToleranceOptions *rayv1.GcsFaultToleranceOptions
		name                     string
		errorMessage             string
		rayStartParams           map[string]string
		envVars                  []corev1.EnvVar
		expectError              bool
	}{
		{
			name: "`redis-username` is set in rayStartParams of the Head Pod",
			rayStartParams: map[string]string{
				"redis-username": "username",
			},
			expectError:  true,
			errorMessage: errorMessageRedisUsername,
		},
		{
			name: "`REDIS_USERNAME` env var is set in the Head Pod",
			envVars: []corev1.EnvVar{
				{
					Name:  REDIS_USERNAME,
					Value: "username",
				},
			},
			expectError:  true,
			errorMessage: errorMessageRedisUsername,
		},
		{
			name: "GcsFaultToleranceOptions.RedisUsername is set",
			gcsFaultToleranceOptions: &rayv1.GcsFaultToleranceOptions{
				RedisAddress: "redis:6379",
				RedisUsername: &rayv1.RedisCredential{
					Value: "username",
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rayCluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					GcsFaultToleranceOptions: tt.gcsFaultToleranceOptions,
					HeadGroupSpec: rayv1.HeadGroupSpec{
						RayStartParams: tt.rayStartParams,
						Template:       podTemplateSpec(tt.envVars, nil),
					},
				},
			}
			err := ValidateRayClusterSpec(&rayCluster.Spec, rayCluster.Annotations)
			if tt.expectError {
				require.Error(t, err)
				assert.EqualError(t, err, tt.errorMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpecNames(t *testing.T) {
	tests := []struct {
		name         string
		errorMessage string
		metadata     metav1.ObjectMeta
		expectError  bool
	}{
		{
			name: "RayCluster name is too long (> MaxRayClusterNameLength characters)",
			metadata: metav1.ObjectMeta{
				Name: strings.Repeat("a", MaxRayClusterNameLength+1),
			},
			expectError:  true,
			errorMessage: fmt.Sprintf("RayCluster name should be no more than %d characters", MaxRayClusterNameLength),
		},
		{
			name: "RayCluster name is ok (== MaxRayClusterNameLength)",
			metadata: metav1.ObjectMeta{
				Name: strings.Repeat("a", MaxRayClusterNameLength),
			},
			expectError: false,
		},
		{
			name: "RayCluster name is not a DNS1035 label",
			metadata: metav1.ObjectMeta{
				Name: strings.Repeat("1", MaxRayClusterNameLength),
			},
			expectError:  true,
			errorMessage: "RayCluster name should be a valid DNS1035 label: [a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRayClusterMetadata(tt.metadata)
			if tt.expectError {
				require.Error(t, err)
				assert.EqualError(t, err, tt.errorMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpecEmptyContainers(t *testing.T) {
	headGroupSpecWithOneContainer := rayv1.HeadGroupSpec{
		Template: podTemplateSpec(nil, nil),
	}
	workerGroupSpecWithOneContainer := rayv1.WorkerGroupSpec{
		Template:    podTemplateSpec(nil, nil),
		MinReplicas: new(int32(0)),
		MaxReplicas: new(int32(5)),
	}
	headGroupSpecWithNoContainers := *headGroupSpecWithOneContainer.DeepCopy()
	headGroupSpecWithNoContainers.Template.Spec.Containers = []corev1.Container{}
	workerGroupSpecWithNoContainers := *workerGroupSpecWithOneContainer.DeepCopy()
	workerGroupSpecWithNoContainers.Template.Spec.Containers = []corev1.Container{}

	tests := []struct {
		rayCluster   *rayv1.RayCluster
		name         string
		errorMessage string
		expectError  bool
	}{
		{
			name: "headGroupSpec has no containers",
			rayCluster: &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec: headGroupSpecWithNoContainers,
				},
			},
			expectError:  true,
			errorMessage: "headGroupSpec should have at least one container",
		},
		{
			name: "workerGroupSpec has no containers",
			rayCluster: &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec:    headGroupSpecWithOneContainer,
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{workerGroupSpecWithNoContainers},
				},
			},
			expectError:  true,
			errorMessage: "workerGroupSpec should have at least one container",
		},
		{
			name: "valid cluster with containers in both head and worker groups",
			rayCluster: &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec:    headGroupSpecWithOneContainer,
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{workerGroupSpecWithOneContainer},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRayClusterSpec(&tt.rayCluster.Spec, tt.rayCluster.Annotations)
			if tt.expectError {
				require.Error(t, err)
				assert.EqualError(t, err, tt.errorMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpecSuspendingWorkerGroup(t *testing.T) {
	headGroupSpec := rayv1.HeadGroupSpec{
		Template: podTemplateSpec(nil, nil),
	}
	workerGroupSpecSuspended := rayv1.WorkerGroupSpec{
		GroupName:   "worker-group-1",
		Template:    podTemplateSpec(nil, nil),
		MinReplicas: new(int32(0)),
		MaxReplicas: new(int32(5)),
	}
	workerGroupSpecSuspended.Suspend = new(true)

	tests := []struct {
		rayCluster   *rayv1.RayCluster
		name         string
		errorMessage string
		expectError  bool
		featureGate  bool
	}{
		{
			name: "suspend without autoscaler and the feature gate",
			rayCluster: &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec:    headGroupSpec,
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{workerGroupSpecSuspended},
				},
			},
			featureGate:  false,
			expectError:  true,
			errorMessage: fmt.Sprintf("worker group %s can be suspended only when the RayJobDeletionPolicy feature gate is enabled", workerGroupSpecSuspended.GroupName),
		},
		{
			name: "suspend without autoscaler",
			rayCluster: &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec:    headGroupSpec,
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{workerGroupSpecSuspended},
				},
			},
			featureGate: true,
			expectError: false,
		},
		{
			// TODO (rueian): This can be supported in future Ray. We should check the RayVersion once we know the version.
			name: "suspend with autoscaler",
			rayCluster: &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HeadGroupSpec:           headGroupSpec,
					WorkerGroupSpecs:        []rayv1.WorkerGroupSpec{workerGroupSpecSuspended},
					EnableInTreeAutoscaling: new(true),
				},
			},
			featureGate:  true,
			expectError:  true,
			errorMessage: fmt.Sprintf("worker group %s cannot be suspended with Autoscaler enabled", workerGroupSpecSuspended.GroupName),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, tt.featureGate)
			err := ValidateRayClusterSpec(&tt.rayCluster.Spec, tt.rayCluster.Annotations)
			if tt.expectError {
				require.Error(t, err)
				assert.EqualError(t, err, tt.errorMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func podTemplateSpec(envVars []corev1.EnvVar, restartPolicy *corev1.RestartPolicy) corev1.PodTemplateSpec {
	spec := corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Env: envVars,
				},
			},
		},
	}

	if restartPolicy != nil {
		spec.Spec.RestartPolicy = *restartPolicy
	}

	return spec
}

func TestValidateRayClusterSpecAutoscaler(t *testing.T) {
	tests := map[string]struct {
		expectedErr string
		spec        rayv1.RayClusterSpec
	}{
		"should return error if autoscaler is enabled and any worker group is suspended": {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, nil),
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName: "worker-group-1",
						Template:  podTemplateSpec(nil, nil),
						Suspend:   new(true),
					},
				},
			},
			expectedErr: "worker group worker-group-1 cannot be suspended with Autoscaler enabled",
		},
		fmt.Sprintf("should return error if %s env var is set to '1' when autoscaler is disabled", RAY_ENABLE_AUTOSCALER_V2): {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(false),
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec([]corev1.EnvVar{
						{
							Name:  RAY_ENABLE_AUTOSCALER_V2,
							Value: "1",
						},
					}, nil),
				},
			},
			expectedErr: fmt.Sprintf("environment variable %s cannot be set to '1' when enableInTreeAutoscaling is false. Please set enableInTreeAutoscaling: true to use autoscaler v2", RAY_ENABLE_AUTOSCALER_V2),
		},
		fmt.Sprintf("should return error if %s env var is set to 'true' when autoscaler is disabled", RAY_ENABLE_AUTOSCALER_V2): {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(false),
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec([]corev1.EnvVar{
						{
							Name:  RAY_ENABLE_AUTOSCALER_V2,
							Value: "true",
						},
					}, nil),
				},
			},
			expectedErr: fmt.Sprintf("environment variable %s cannot be set to 'true' when enableInTreeAutoscaling is false. Please set enableInTreeAutoscaling: true to use autoscaler v2", RAY_ENABLE_AUTOSCALER_V2),
		},
		fmt.Sprintf("should return error if autoscaler v2 is enabled and head Pod has env var %s", RAY_ENABLE_AUTOSCALER_V2): {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec([]corev1.EnvVar{
						{
							Name:  RAY_ENABLE_AUTOSCALER_V2,
							Value: "true",
						},
					}, nil),
				},
			},
			expectedErr: fmt.Sprintf("both .spec.autoscalerOptions.version and head Pod env var %s are set, please only use the former", RAY_ENABLE_AUTOSCALER_V2),
		},
		"should return error if autoscaler v2 is enabled and head Pod has a restartPolicy other than Never or unset": {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, ptr.To(corev1.RestartPolicyAlways)),
				},
			},
			expectedErr: "restartPolicy for head Pod should be Never or unset when using autoscaler V2",
		},
		"should not return error if autoscaler v1 is enabled and a worker group has a restartPolicy other than Never or unset (warning only)": {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV1),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, nil),
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName: "worker-group-1",
						Template:  podTemplateSpec(nil, ptr.To(corev1.RestartPolicyNever)),
					},
					{
						GroupName: "worker-group-2",
						Template:  podTemplateSpec(nil, ptr.To(corev1.RestartPolicyAlways)),
					},
				},
			},
			// No error expected; this is now a warning surfaced.
		},
		"should not return error if autoscaler v1 is enabled and all worker groups have restartPolicy Never or unset": {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV1),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, nil),
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName: "worker-group-1",
						Template:  podTemplateSpec(nil, nil),
					},
					{
						GroupName: "worker-group-2",
						Template:  podTemplateSpec(nil, ptr.To(corev1.RestartPolicyNever)),
					},
				},
			},
		},
		"should return error if autoscaler v2 is enabled and a worker group has a restartPolicy other than Never or unset": {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, ptr.To(corev1.RestartPolicyNever)),
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName: "worker-group-1",
						Template:  podTemplateSpec(nil, ptr.To(corev1.RestartPolicyNever)),
					},
					{
						GroupName: "worker-group-2",
						Template:  podTemplateSpec(nil, ptr.To(corev1.RestartPolicyAlways)),
					},
				},
			},
			expectedErr: "restartPolicy for worker group worker-group-2 should be Never or unset when using autoscaler V2",
		},
		"should not return error if autoscaler configs are valid": {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, nil),
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName: "worker-group-1",
						Template:  podTemplateSpec(nil, nil),
					},
					{
						GroupName: "worker-group-2",
						Template:  podTemplateSpec(nil, ptr.To(corev1.RestartPolicyNever)),
					},
				},
			},
		},
		fmt.Sprintf("should return error if %s is set in autoscalerOptions.env (args also set)", KUBERAY_GEN_AUTOSCALER_START_CMD): {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Args: []string{"my-custom-autoscaler-cmd"},
					Env: []corev1.EnvVar{
						{
							Name:  KUBERAY_GEN_AUTOSCALER_START_CMD,
							Value: "ray kuberay-autoscaler --cluster-name foo --cluster-namespace bar",
						},
					},
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, nil),
				},
			},
			expectedErr: fmt.Sprintf(
				"autoscalerOptions.env must not contain %s: "+
					"it is managed by KubeRay and injected automatically into the autoscaler container",
				KUBERAY_GEN_AUTOSCALER_START_CMD,
			),
		},
		fmt.Sprintf("should return error if %s is set in autoscalerOptions.env even when autoscalerOptions.args is absent", KUBERAY_GEN_AUTOSCALER_START_CMD): {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					// No Args — KUBERAY_GEN_AUTOSCALER_START_CMD is always KubeRay-managed,
					// so it must never be set by the user regardless of whether Args is present.
					Env: []corev1.EnvVar{
						{
							Name:  KUBERAY_GEN_AUTOSCALER_START_CMD,
							Value: "ray kuberay-autoscaler --cluster-name foo --cluster-namespace bar",
						},
					},
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, nil),
				},
			},
			expectedErr: fmt.Sprintf(
				"autoscalerOptions.env must not contain %s: "+
					"it is managed by KubeRay and injected automatically into the autoscaler container",
				KUBERAY_GEN_AUTOSCALER_START_CMD,
			),
		},
	}

	features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, true)
	defer func() {
		features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, false)
	}()

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateRayClusterSpec(&tc.spec, map[string]string{})
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateRayClusterSpecAutoscaler_FlexibleRestartPolicy tests that when Ray version is
// 2.56.0 or later, non-Never RestartPolicy values are accepted for head and worker pods
// even when autoscaler V2 is active.
func TestValidateRayClusterSpecAutoscaler_FlexibleRestartPolicy(t *testing.T) {
	tests := map[string]struct {
		expectedErr string
		spec        rayv1.RayClusterSpec
	}{
		"should not return error if Ray >= 2.56.0 and head Pod has a non-Never restartPolicy": {
			spec: rayv1.RayClusterSpec{
				RayVersion:              "2.56.0",
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, ptr.To(corev1.RestartPolicyAlways)),
				},
			},
		},
		"should not return error if Ray >= 2.56.0 and a worker group has a non-Never restartPolicy": {
			spec: rayv1.RayClusterSpec{
				RayVersion:              "2.56.0",
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, ptr.To(corev1.RestartPolicyAlways)),
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName: "worker-group-1",
						Template:  podTemplateSpec(nil, ptr.To(corev1.RestartPolicyOnFailure)),
					},
					{
						GroupName: "worker-group-2",
						Template:  podTemplateSpec(nil, ptr.To(corev1.RestartPolicyAlways)),
					},
				},
			},
		},
		"should not return error if Ray >= 2.56.0 and restartPolicy is unset": {
			spec: rayv1.RayClusterSpec{
				RayVersion:              "2.56.0",
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, nil),
				},
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName: "worker-group-1",
						Template:  podTemplateSpec(nil, nil),
					},
				},
			},
		},
		"should return error if Ray < 2.56.0 and head Pod has a non-Never restartPolicy": {
			spec: rayv1.RayClusterSpec{
				RayVersion:              "2.55.0",
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, ptr.To(corev1.RestartPolicyAlways)),
				},
			},
			expectedErr: "restartPolicy for head Pod should be Never or unset when using autoscaler V2",
		},
		"should return error if Ray version is not specified and head Pod has a non-Never restartPolicy": {
			spec: rayv1.RayClusterSpec{
				EnableInTreeAutoscaling: new(true),
				AutoscalerOptions: &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				},
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, ptr.To(corev1.RestartPolicyAlways)),
				},
			},
			expectedErr: "restartPolicy for head Pod should be Never or unset when using autoscaler V2",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateRayClusterSpec(&tc.spec, map[string]string{})
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpec_IdleTerminationOptions(t *testing.T) {
	createSpec := func() rayv1.RayClusterSpec {
		return rayv1.RayClusterSpec{
			EnableInTreeAutoscaling: new(true),
			RayVersion:              "2.60.0",
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: podTemplateSpec(nil, nil),
			},
		}
	}

	tests := map[string]struct {
		expectedErr string
		spec        rayv1.RayClusterSpec
	}{
		"Valid: idleTerminationOptions.timeoutSeconds with explicit v2": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](600),
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: idleTerminationOptions.timeoutSeconds with v2 enabled via env var": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.HeadGroupSpec.Template = podTemplateSpec([]corev1.EnvVar{
					{Name: RAY_ENABLE_AUTOSCALER_V2, Value: "1"},
				}, nil)
				s.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](600),
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: zero idleTerminationOptions.timeoutSeconds": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](0),
				}
				return s
			}(),
			expectedErr: "",
		},
		"Invalid: negative idleTerminationOptions.timeoutSeconds": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](-1),
				}
				return s
			}(),
			expectedErr: "idleTerminationOptions.timeoutSeconds must be non-negative, got -1",
		},
		"Invalid: idleTerminationOptions.timeoutSeconds without autoscaling": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.EnableInTreeAutoscaling = new(false)
				s.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](600),
				}
				return s
			}(),
			expectedErr: "idleTerminationOptions requires enableInTreeAutoscaling to be true",
		},
		"Invalid: autoscaler v2 not enabled": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](600),
				}
				return s
			}(),
			expectedErr: "idleTerminationOptions requires autoscaler v2. Please set .spec.autoscalerOptions.version to 'v2'",
		},
		"Invalid: idleTerminationOptions.timeoutSeconds with autoscaler v1": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV1),
				}
				s.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](600),
				}
				return s
			}(),
			expectedErr: "idleTerminationOptions requires autoscaler v2. Please set .spec.autoscalerOptions.version to 'v2'",
		},
		"Invalid: idleTerminationOptions.timeoutSeconds with Ray version below minimum": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.RayVersion = "2.59.0"
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](600),
				}
				return s
			}(),
			expectedErr: "idleTerminationOptions requires Ray version 2.60.0 or later, got 2.59.0",
		},
		"Valid: idleTerminationOptions.policy=Delete": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](600),
					Policy:         ptr.To(rayv1.IdleTerminationPolicyDelete),
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: idleTerminationOptions.policy=Suspend": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](600),
					Policy:         ptr.To(rayv1.IdleTerminationPolicySuspend),
				}
				return s
			}(),
			expectedErr: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateRayClusterSpec(&tc.spec, nil)
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpec_Resources(t *testing.T) {
	// Util function to create a RayCluster spec.
	createSpec := func() rayv1.RayClusterSpec {
		return rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: podTemplateSpec(nil, nil),
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:   "worker-group",
					Template:    podTemplateSpec(nil, nil),
					MinReplicas: new(int32(0)),
					MaxReplicas: new(int32(5)),
				},
			},
		}
	}

	tests := []struct {
		name         string
		errorMessage string
		spec         rayv1.RayClusterSpec
		expectError  bool
	}{
		{
			name: "Invalid: Head group has resources in both rayStartParams and top-level Resources",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.HeadGroupSpec.RayStartParams = map[string]string{"num-cpus": "1"}
				s.HeadGroupSpec.Resources = map[string]string{"CPU": "1"}
				return s
			}(),
			expectError:  true,
			errorMessage: "resource fields should not be set in both rayStartParams and Resources for Head group; please use only one",
		},
		{
			name: "Invalid: Worker group has resources in both rayStartParams and .Resources",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs[0].RayStartParams = map[string]string{"num-gpus": "1"}
				s.WorkerGroupSpecs[0].Resources = map[string]string{"GPU": "1"}
				return s
			}(),
			expectError:  true,
			errorMessage: "resource fields should not be set in both rayStartParams and Resources for worker-group group; please use only one",
		},
		{
			name: "Valid: Only rayStartParams resources are set for head",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.HeadGroupSpec.RayStartParams = map[string]string{
					"num-cpus":  "2",
					"memory":    "4G",
					"resources": "{\"TPU\": \"8\"}",
				}
				return s
			}(),
			expectError: false,
		},
		{
			name: "Valid: Only .Resources field is set for worker",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs[0].Resources = map[string]string{"CPU": "2", "memory": "4G", "TPU": "8"}
				return s
			}(),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRayClusterSpec(&tt.spec, nil)
			if tt.expectError {
				require.Error(t, err)
				assert.EqualError(t, err, tt.errorMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpec_Labels(t *testing.T) {
	// Util function to create a RayCluster spec.
	createSpec := func() rayv1.RayClusterSpec {
		return rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: podTemplateSpec(nil, nil),
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:   "worker-group",
					Template:    podTemplateSpec(nil, nil),
					MinReplicas: new(int32(0)),
					MaxReplicas: new(int32(5)),
				},
			},
		}
	}

	tests := []struct {
		name         string
		errorMessage string
		spec         rayv1.RayClusterSpec
		expectError  bool
	}{
		{
			name: "Invalid: Head group has 'labels' in rayStartParams",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.HeadGroupSpec.RayStartParams = map[string]string{"labels": "ray.io/node-group=worker-group-1"}
				return s
			}(),
			expectError:  true,
			errorMessage: "rayStartParams['labels'] is not supported for Head group; please use the top-level Labels field instead",
		},
		{
			name: "Invalid: Worker group has 'labels' in rayStartParams",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs[0].RayStartParams = map[string]string{"labels": "ray.io/node-group=worker-group-1"}
				return s
			}(),
			expectError:  true,
			errorMessage: "rayStartParams['labels'] is not supported for worker-group group; please use the top-level Labels field instead",
		},
		{
			name: "Valid: Only .Labels field is set",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.HeadGroupSpec.Labels = map[string]string{"ray.io/market-type": "on-demand"}
				s.WorkerGroupSpecs[0].Labels = map[string]string{"ray.io/accelerator-type": "TPU-V6E"}
				return s
			}(),
			expectError: false,
		},
		{
			name: "Invalid: Label key does not follow Kubernetes syntax",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs[0].Labels = map[string]string{"invalid_key!": "value"}
				return s
			}(),
			expectError:  true,
			errorMessage: "invalid label key for worker-group group: 'invalid_key!', error: name part must consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character",
		},
		{
			name: "Invalid: Label value does not follow Kubernetes syntax",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.HeadGroupSpec.Labels = map[string]string{"valid-key": "invalid/value"}
				return s
			}(),
			expectError:  true,
			errorMessage: "invalid label value for key 'valid-key' in Head group: 'invalid/value', error: a valid label must be an empty string or consist of alphanumeric characters, '-', '_' or '.', and must start and end with an alphanumeric character",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRayClusterSpec(&tt.spec, nil)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpecRayVersionForAuth(t *testing.T) {
	tests := []struct {
		name               string
		rayVersion         string
		errorMessage       string
		enableK8sTokenAuth bool
		expectError        bool
	}{
		{
			name:        "Valid Ray version 2.52.0",
			rayVersion:  "2.52.0",
			expectError: false,
		},
		{
			name:               "Invalid Ray version 2.52.0 with k8s token auth",
			rayVersion:         "2.52.0",
			enableK8sTokenAuth: true,
			expectError:        true,
		},
		{
			name:               "Valid Ray version 2.55.0 with k8s token auth",
			rayVersion:         "2.55.0",
			enableK8sTokenAuth: true,
			expectError:        false,
		},
		{
			name:        "Valid Ray version 3.0.0",
			rayVersion:  "3.0.0",
			expectError: false,
		},
		{
			name:               "Valid Ray version 3.0.0 with k8s token auth",
			rayVersion:         "3.0.0",
			enableK8sTokenAuth: true,
			expectError:        false,
		},
		{
			name:         "Invalid Ray version 2.50.0",
			rayVersion:   "2.50.0",
			expectError:  true,
			errorMessage: "authOptions.mode is 'token' but minimum Ray version is 2.52.0, got 2.50.0",
		},
		{
			name:         "Invalid Ray version format",
			rayVersion:   "invalid-version",
			expectError:  true,
			errorMessage: "authOptions.mode is 'token' but RayVersion format is invalid: invalid-version, could not parse \"invalid-version\" as version",
		},
		{
			name:         "Empty Ray version",
			rayVersion:   "",
			expectError:  true,
			errorMessage: "authOptions.mode is 'token' but RayVersion was not specified. Ray version 2.52.0 or later is required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &rayv1.RayClusterSpec{
				RayVersion: tt.rayVersion,
				HeadGroupSpec: rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, nil),
				},
				AuthOptions: &rayv1.AuthOptions{
					Mode:               rayv1.AuthModeToken,
					EnableK8sTokenAuth: new(tt.enableK8sTokenAuth),
				},
			}
			err := ValidateRayClusterSpec(spec, nil)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayJobStatus(t *testing.T) {
	tests := []struct {
		name        string
		jobSpec     rayv1.RayJobSpec
		jobStatus   rayv1.RayJobStatus
		expectError bool
	}{
		{
			name: "JobDeploymentStatus is Waiting and SubmissionMode is not InteractiveMode",
			jobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusWaiting,
			},
			jobSpec: rayv1.RayJobSpec{
				SubmissionMode: rayv1.K8sJobMode,
			},
			expectError: true,
		},
		{
			name: "JobDeploymentStatus is Waiting and SubmissionMode is InteractiveMode",
			jobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusWaiting,
			},
			jobSpec: rayv1.RayJobSpec{
				SubmissionMode: rayv1.InteractiveMode,
			},
			expectError: false,
		},
		{
			name: "JobDeploymentStatus is not Waiting and SubmissionMode is not InteractiveMode",
			jobStatus: rayv1.RayJobStatus{
				JobDeploymentStatus: rayv1.JobDeploymentStatusRunning,
			},
			jobSpec: rayv1.RayJobSpec{
				SubmissionMode: rayv1.HTTPMode,
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rayJob := &rayv1.RayJob{
				Status: tt.jobStatus,
				Spec:   tt.jobSpec,
			}
			err := ValidateRayJobStatus(rayJob)
			if (err != nil) != tt.expectError {
				t.Errorf("ValidateRayJobStatus() error = %v, wantErr %v", err, tt.expectError)
			}
		})
	}
}

func TestValidateRayJobSpec(t *testing.T) {
	tests := []struct {
		name        string
		spec        rayv1.RayJobSpec
		expectError bool
	}{
		{
			name:        "one of RayClusterSpec or ClusterSelector must be set",
			spec:        rayv1.RayJobSpec{},
			expectError: true,
		},
		{
			name: "a RayJob with shutdownAfterJobFinishes set to false is not allowed to be suspended",
			spec: rayv1.RayJobSpec{
				Suspend:                  true,
				ShutdownAfterJobFinishes: false,
			},
			expectError: true,
		},
		{
			name: "valid RayJob",
			spec: rayv1.RayJobSpec{
				Suspend:                  true,
				ShutdownAfterJobFinishes: true,
				RayClusterSpec:           createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "RayJob does not support idleTerminationOptions",
			spec: func() rayv1.RayJobSpec {
				clusterSpec := createBasicRayClusterSpec()
				clusterSpec.IdleTerminationOptions = &rayv1.IdleTerminationOptions{
					TimeoutSeconds: ptr.To[int32](600),
				}
				return rayv1.RayJobSpec{
					ShutdownAfterJobFinishes: true,
					RayClusterSpec:           clusterSpec,
				}
			}(),
			expectError: true,
		},
		{
			name: "the ClusterSelector mode doesn't support the suspend operation",
			spec: rayv1.RayJobSpec{
				Suspend:                  true,
				ShutdownAfterJobFinishes: true,
				ClusterSelector: map[string]string{
					"key": "value",
				},
			},
			expectError: true,
		},
		{
			name: "failed to unmarshal RuntimeEnvYAML",
			spec: rayv1.RayJobSpec{
				RuntimeEnvYAML: "invalid_yaml_str",
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "backoffLimit must be a positive integer",
			spec: rayv1.RayJobSpec{
				BackoffLimit:   ptr.To[int32](-1),
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "BackoffLimit is incompatible with InteractiveMode",
			spec: rayv1.RayJobSpec{
				BackoffLimit:   ptr.To[int32](1),
				SubmissionMode: rayv1.InteractiveMode,
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "BackoffLimit is 0 and SubmissionMode is InteractiveMode",
			spec: rayv1.RayJobSpec{
				BackoffLimit:   ptr.To[int32](0),
				SubmissionMode: rayv1.InteractiveMode,
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "BackoffLimit is nil and SubmissionMode is InteractiveMode",
			spec: rayv1.RayJobSpec{
				BackoffLimit:   nil,
				SubmissionMode: rayv1.InteractiveMode,
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "ShutdownAfterJobFinishes is false and TTLSecondsAfterFinished is not zero",
			spec: rayv1.RayJobSpec{
				ShutdownAfterJobFinishes: false,
				TTLSecondsAfterFinished:  5,
				RayClusterSpec:           createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "ShutdownAfterJobFinishes is true and TTLSecondsAfterFinished is not zero",
			spec: rayv1.RayJobSpec{
				ShutdownAfterJobFinishes: true,
				TTLSecondsAfterFinished:  5,
				RayClusterSpec:           createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "ShutdownAfterJobFinishes is true and TTLSecondsAfterFinished is negative",
			spec: rayv1.RayJobSpec{
				ShutdownAfterJobFinishes: true,
				TTLSecondsAfterFinished:  -5,
				RayClusterSpec:           createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "SidecarMode",
			spec: rayv1.RayJobSpec{
				SubmissionMode: rayv1.SidecarMode,
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "SidecarMode doesn't support SubmitterPodTemplate",
			spec: rayv1.RayJobSpec{
				SubmissionMode: rayv1.SidecarMode,
				SubmitterPodTemplate: &corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{},
				},
			},
			expectError: true,
		},
		{
			name: "SidecarMode doesn't support SubmitterConfig",
			spec: rayv1.RayJobSpec{
				SubmissionMode: rayv1.SidecarMode,
				SubmitterConfig: &rayv1.SubmitterConfig{
					BackoffLimit: ptr.To[int32](1),
				},
			},
			expectError: true,
		},
		{
			name: "SidecarMode RayCluster head pod should only be Never or unset",
			spec: rayv1.RayJobSpec{
				SubmissionMode: rayv1.SidecarMode,
				RayClusterSpec: &rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: podTemplateSpec(nil, ptr.To(corev1.RestartPolicyAlways)),
					},
				},
			},
			expectError: true,
		},
		{
			name: "SidecarMode doesn't support ClusterSelector",
			spec: rayv1.RayJobSpec{
				SubmissionMode:  rayv1.SidecarMode,
				ClusterSelector: map[string]string{"ray.io/cluster": "ray-cluster"},
			},
			expectError: true,
		},
		{
			name: "failed to get cluster name in ClusterSelector map",
			spec: rayv1.RayJobSpec{
				ClusterSelector: map[string]string{},
			},
			expectError: true,
		},
		{
			name: "cluster name in ClusterSelector is empty",
			spec: rayv1.RayJobSpec{
				ClusterSelector: map[string]string{"ray.io/cluster": ""},
			},
			expectError: true,
		},
		{
			name: "cluster name in ClusterSelector is not empty",
			spec: rayv1.RayJobSpec{
				ClusterSelector: map[string]string{"ray.io/cluster": "ray-cluster"},
			},
			expectError: false,
		},
		{
			name: "BackoffLimit > 0 is incompatible with ClusterSelector mode",
			spec: rayv1.RayJobSpec{
				ClusterSelector: map[string]string{"ray.io/cluster": "ray-cluster"},
				BackoffLimit:    ptr.To[int32](1),
			},
			expectError: true,
		},
		{
			name: "RayJob does not support K8s token auth mode",
			spec: rayv1.RayJobSpec{
				RayClusterSpec: &rayv1.RayClusterSpec{
					AuthOptions: &rayv1.AuthOptions{
						Mode:               rayv1.AuthModeToken,
						EnableK8sTokenAuth: new(true),
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRayJobSpec(&rayv1.RayJob{
				Spec: tt.spec,
			})
			if tt.expectError {
				require.Error(t, err, tt.name)
			} else {
				require.NoError(t, err, tt.name)
			}
		})
	}
}

func TestValidateRayJobSpecWithFeatureGate(t *testing.T) {
	headGroupSpecWithOneContainer := rayv1.HeadGroupSpec{
		Template: podTemplateSpec(nil, nil),
	}

	tests := []struct {
		name        string
		spec        rayv1.RayJobSpec
		expectError bool
	}{
		// Legacy DeletionStrategy tests
		{
			name: "the ClusterSelector mode doesn't support DeletionStrategy=DeleteCluster",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteCluster),
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteCluster),
					},
				}, ClusterSelector: map[string]string{"key": "value"},
			},
			expectError: true,
		},
		{
			name: "the ClusterSelector mode doesn't support DeletionStrategy=DeleteWorkers",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteWorkers),
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteWorkers),
					},
				}, ClusterSelector: map[string]string{"key": "value"},
			},
			expectError: true,
		},
		{
			name: "DeletionStrategy=DeleteWorkers currently does not support RayCluster with autoscaling enabled",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteWorkers),
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteWorkers),
					},
				}, RayClusterSpec: &rayv1.RayClusterSpec{
					EnableInTreeAutoscaling: new(true),
					HeadGroupSpec:           headGroupSpecWithOneContainer,
				},
			},
			expectError: true,
		},
		{
			name: "valid RayJob with DeletionStrategy=DeleteCluster",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteCluster),
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteCluster),
					},
				}, ShutdownAfterJobFinishes: true,
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "valid RayJob without DeletionStrategy",
			spec: rayv1.RayJobSpec{
				DeletionStrategy:         nil,
				ShutdownAfterJobFinishes: true,
				RayClusterSpec:           createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "shutdownAfterJobFinshes is set to 'true' while deletion policy is 'DeleteNone'",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteNone),
					},
					OnFailure: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteNone),
					},
				}, ShutdownAfterJobFinishes: true,
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "OnSuccess unset",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					OnFailure: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteNone),
					},
				}, ShutdownAfterJobFinishes: true,
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "OnSuccess.DeletionPolicyType unset",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					OnFailure: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteNone),
					},
				}, ShutdownAfterJobFinishes: true,
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "OnFailure unset",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteNone),
					},
				}, ShutdownAfterJobFinishes: true,
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "OnFailure.DeletionPolicyType unset",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteNone),
					},
					OnFailure: &rayv1.DeletionPolicy{},
				}, ShutdownAfterJobFinishes: true,
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "headGroupSpec should have at least one container",
			spec: rayv1.RayJobSpec{
				RayClusterSpec: &rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{},
				},
			},
			expectError: true,
		},
		// New Deletion Rules tests
		{
			name: "valid deletionRules",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "deletionRules and ShutdownAfterJobFinishes both set",
			spec: rayv1.RayJobSpec{
				ShutdownAfterJobFinishes: true,
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "deletionRules and legacy onSuccess both set",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					OnSuccess: &rayv1.DeletionPolicy{
						Policy: ptr.To(rayv1.DeleteCluster),
					},
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "nil DeletionStrategy",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{},
				RayClusterSpec:   createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "empty DeletionStrategy",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "duplicate rule in deletionRules",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 20,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "negative TTLSeconds in deletionRules",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: -10,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "deletionRules with ClusterSelector and DeleteWorkers policy",
			spec: rayv1.RayJobSpec{
				ClusterSelector: map[string]string{"key": "value"},
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "deletionRules with ClusterSelector and DeleteCluster policy",
			spec: rayv1.RayJobSpec{
				ClusterSelector: map[string]string{"key": "value"},
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "deletionRules with autoscaling and DeleteWorkers policy",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
					},
				},
				RayClusterSpec: &rayv1.RayClusterSpec{
					EnableInTreeAutoscaling: new(true),
					HeadGroupSpec:           headGroupSpecWithOneContainer,
				},
			},
			expectError: true,
		},
		{
			name: "inconsistent TTLs in deletionRules (DeleteCluster < DeleteWorkers)",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 20,
							},
						},
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "inconsistent TTLs in deletionRules (DeleteSelf < DeleteCluster)",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 20,
							},
						},
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "valid complex deletionRules",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 10,
							},
						},
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 20,
							},
						},
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusSucceeded),
								TTLSeconds: 30,
							},
						},
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusFailed),
								TTLSeconds: 0,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "valid deletionRules with JobDeploymentStatus=Failed",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusFailed),
								TTLSeconds:          10,
							},
						},
						{
							Policy: rayv1.DeleteSelf,
							Condition: rayv1.DeletionCondition{
								JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusFailed),
								TTLSeconds:          20,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "invalid: both JobStatus and JobDeploymentStatus set",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobStatus:           ptr.To(rayv1.JobStatusFailed),
								JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusFailed),
								TTLSeconds:          10,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "invalid: neither JobStatus nor JobDeploymentStatus set",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								TTLSeconds: 10,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "duplicate rule with JobDeploymentStatus",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusFailed),
								TTLSeconds:          10,
							},
						},
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusFailed),
								TTLSeconds:          20,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name: "valid: mixed JobStatus and JobDeploymentStatus rules",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobStatus:  ptr.To(rayv1.JobStatusFailed),
								TTLSeconds: 10,
							},
						},
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusFailed),
								TTLSeconds:          20,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "inconsistent TTLs with JobDeploymentStatus (DeleteCluster < DeleteWorkers)",
			spec: rayv1.RayJobSpec{
				DeletionStrategy: &rayv1.DeletionStrategy{
					DeletionRules: []rayv1.DeletionRule{
						{
							Policy: rayv1.DeleteWorkers,
							Condition: rayv1.DeletionCondition{
								JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusFailed),
								TTLSeconds:          20,
							},
						},
						{
							Policy: rayv1.DeleteCluster,
							Condition: rayv1.DeletionCondition{
								JobDeploymentStatus: ptr.To(rayv1.JobDeploymentStatusFailed),
								TTLSeconds:          10,
							},
						},
					},
				},
				RayClusterSpec: createBasicRayClusterSpec(),
			},
			expectError: true,
		},
	}

	features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, true)
	defer func() {
		features.SetFeatureGateDuringTest(t, features.RayJobDeletionPolicy, false)
	}()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRayJobSpec(&rayv1.RayJob{
				Spec: tt.spec,
			})
			if tt.expectError {
				require.Error(t, err, tt.name)
			} else {
				require.NoError(t, err, tt.name)
			}
		})
	}
}

func TestValidateRayJobMetadata(t *testing.T) {
	err := ValidateRayJobMetadata(metav1.ObjectMeta{
		Name: strings.Repeat("j", MaxRayJobNameLength+1),
	})
	require.ErrorContains(t, err, fmt.Sprintf("RayJob name should be no more than %d characters", MaxRayJobNameLength))

	err = ValidateRayJobMetadata(metav1.ObjectMeta{
		Name: strings.Repeat("1", MaxRayJobNameLength),
	})
	require.ErrorContains(t, err, "RayJob name should be a valid DNS1035 label: [a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')]")

	err = ValidateRayJobMetadata(metav1.ObjectMeta{
		Name: strings.Repeat("j", MaxRayJobNameLength),
	})
	require.NoError(t, err)
}

func TestValidateRayServiceSpec(t *testing.T) {
	upgradeStrat := rayv1.RayServiceUpgradeType("invalidStrategy")

	tests := []struct {
		name              string
		unexpectedMessage string
		spec              rayv1.RayServiceSpec
		expectError       bool
	}{
		{
			name: "spec.rayClusterConfig.headGroupSpec.headService.metadata.name should not be set",
			spec: rayv1.RayServiceSpec{
				RayClusterSpec: rayv1.RayClusterSpec{
					HeadGroupSpec: rayv1.HeadGroupSpec{
						HeadService: &corev1.Service{
							ObjectMeta: metav1.ObjectMeta{
								Name: "my-head-service",
							},
						},
					},
				},
			},
			expectError: true,
		},
		{
			name: "The RayService spec is valid.",
			spec: rayv1.RayServiceSpec{
				RayClusterSpec: *createBasicRayClusterSpec(),
			},
			expectError: false,
		},
		{
			name: "spec.UpgradeSpec.Type is invalid",
			spec: rayv1.RayServiceSpec{
				UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
					Type: &upgradeStrat,
				},
				RayClusterSpec: *createBasicRayClusterSpec(),
			},
			expectError: true,
		},
		{
			name:        "headGroupSpec should have at least one container",
			spec:        rayv1.RayServiceSpec{},
			expectError: true,
		},
		{
			name: "Spec.RayClusterDeletionDelaySeconds is negative",
			spec: rayv1.RayServiceSpec{
				RayClusterSpec:                 *createBasicRayClusterSpec(),
				RayClusterDeletionDelaySeconds: ptr.To[int32](-1),
			},
			expectError: true,
		},
		{
			name: "RayService does not support K8s token auth mode",
			spec: rayv1.RayServiceSpec{
				RayClusterSpec: rayv1.RayClusterSpec{
					AuthOptions: &rayv1.AuthOptions{
						Mode:               rayv1.AuthModeToken,
						EnableK8sTokenAuth: new(true),
					},
				},
			},
			expectError: true,
		},
		{
			name: "RayService does not support idleTerminationOptions",
			spec: rayv1.RayServiceSpec{
				RayClusterSpec: rayv1.RayClusterSpec{
					IdleTerminationOptions: &rayv1.IdleTerminationOptions{
						TimeoutSeconds: ptr.To[int32](600),
					},
				},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRayServiceSpec(&rayv1.RayService{
				Spec: tt.spec,
			})
			if tt.expectError {
				require.Error(t, err, tt.name)
			} else {
				require.NoError(t, err, tt.name)
			}
		})
	}
}

func TestValidateRayServiceMetadata(t *testing.T) {
	err := ValidateRayServiceMetadata(metav1.ObjectMeta{
		Name: strings.Repeat("j", MaxRayServiceNameLength+1),
	})
	require.ErrorContains(t, err, fmt.Sprintf("RayService name should be no more than %d characters", MaxRayServiceNameLength))

	err = ValidateRayServiceMetadata(metav1.ObjectMeta{
		Name: strings.Repeat("1", MaxRayServiceNameLength),
	})
	require.ErrorContains(t, err, "RayService name should be a valid DNS1035 label: [a DNS-1035 label must consist of lower case alphanumeric characters or '-', start with an alphabetic character, and end with an alphanumeric character (e.g. 'my-name',  or 'abc-123', regex used for validation is '[a-z]([-a-z0-9]*[a-z0-9])?')]")

	err = ValidateRayServiceMetadata(metav1.ObjectMeta{
		Name: strings.Repeat("j", MaxRayServiceNameLength),
	})
	require.NoError(t, err)

	// Test valid initializing timeout annotations
	err = ValidateRayServiceMetadata(metav1.ObjectMeta{
		Name: "valid-service",
		Annotations: map[string]string{
			RayServiceInitializingTimeoutAnnotation: "5m",
		},
	})
	require.NoError(t, err)

	err = ValidateRayServiceMetadata(metav1.ObjectMeta{
		Name: "valid-service",
		Annotations: map[string]string{
			RayServiceInitializingTimeoutAnnotation: "300",
		},
	})
	require.NoError(t, err)

	// Test invalid initializing timeout annotations
	err = ValidateRayServiceMetadata(metav1.ObjectMeta{
		Name: "invalid-service",
		Annotations: map[string]string{
			RayServiceInitializingTimeoutAnnotation: "0",
		},
	})
	require.ErrorContains(t, err, "must be a positive")

	err = ValidateRayServiceMetadata(metav1.ObjectMeta{
		Name: "invalid-service",
		Annotations: map[string]string{
			RayServiceInitializingTimeoutAnnotation: "-100",
		},
	})
	require.ErrorContains(t, err, "must be a positive")

	err = ValidateRayServiceMetadata(metav1.ObjectMeta{
		Name: "invalid-service",
		Annotations: map[string]string{
			RayServiceInitializingTimeoutAnnotation: "-5m",
		},
	})
	require.ErrorContains(t, err, "must be a positive duration")

	err = ValidateRayServiceMetadata(metav1.ObjectMeta{
		Name: "invalid-service",
		Annotations: map[string]string{
			RayServiceInitializingTimeoutAnnotation: "invalid",
		},
	})
	require.ErrorContains(t, err, "invalid format")
}

func createBasicRayClusterSpec() *rayv1.RayClusterSpec {
	return &rayv1.RayClusterSpec{
		HeadGroupSpec: rayv1.HeadGroupSpec{
			Template: podTemplateSpec(nil, nil),
		},
	}
}

func TestValidateClusterUpgradeOptions(t *testing.T) {
	tests := []struct {
		maxSurgePercent   *int32
		stepSizePercent   *int32
		intervalSeconds   *int32
		name              string
		gatewayClassName  string
		gatewayRef        *rayv1.GatewayReference
		spec              rayv1.RayServiceSpec
		enableAutoscaling bool
		expectError       bool
	}{
		{
			name:              "valid config",
			maxSurgePercent:   new(int32(50)),
			stepSizePercent:   new(int32(50)),
			intervalSeconds:   new(int32(10)),
			gatewayClassName:  "istio",
			enableAutoscaling: true,
			expectError:       false,
		},
		{
			name:              "missing autoscaler",
			maxSurgePercent:   new(int32(50)),
			stepSizePercent:   new(int32(50)),
			intervalSeconds:   new(int32(10)),
			gatewayClassName:  "istio",
			enableAutoscaling: false,
			expectError:       true,
		},
		{
			name:              "missing options",
			enableAutoscaling: true,
			expectError:       true,
		},
		{
			name:              "invalid MaxSurgePercent",
			maxSurgePercent:   new(int32(200)),
			stepSizePercent:   new(int32(50)),
			intervalSeconds:   new(int32(10)),
			gatewayClassName:  "istio",
			enableAutoscaling: true,
			expectError:       true,
		},
		{
			name:              "missing StepSizePercent",
			maxSurgePercent:   new(int32(50)),
			intervalSeconds:   new(int32(10)),
			gatewayClassName:  "istio",
			enableAutoscaling: true,
			expectError:       true,
		},
		{
			name:              "invalid StepSizePercent (greater than MaxSurgePercent)",
			maxSurgePercent:   new(int32(50)),
			stepSizePercent:   new(int32(75)),
			intervalSeconds:   new(int32(10)),
			gatewayClassName:  "istio",
			enableAutoscaling: true,
			expectError:       true,
		},
		{
			name:              "invalid IntervalSeconds",
			maxSurgePercent:   new(int32(50)),
			stepSizePercent:   new(int32(50)),
			intervalSeconds:   new(int32(0)),
			gatewayClassName:  "istio",
			enableAutoscaling: true,
			expectError:       true,
		},
		{
			name:              "valid GatewayRef instead of GatewayClassName",
			maxSurgePercent:   new(int32(50)),
			stepSizePercent:   new(int32(50)),
			intervalSeconds:   new(int32(10)),
			gatewayRef:        &rayv1.GatewayReference{Name: "shared-gw", Namespace: "gateways"},
			enableAutoscaling: true,
			expectError:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var upgradeStrategy *rayv1.RayServiceUpgradeStrategy
			if tt.maxSurgePercent != nil || tt.stepSizePercent != nil || tt.intervalSeconds != nil || tt.gatewayClassName != "" || tt.gatewayRef != nil {
				upgradeStrategy = &rayv1.RayServiceUpgradeStrategy{
					Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
					ClusterUpgradeOptions: &rayv1.ClusterUpgradeOptions{
						MaxSurgePercent:  tt.maxSurgePercent,
						StepSizePercent:  tt.stepSizePercent,
						IntervalSeconds:  tt.intervalSeconds,
						GatewayClassName: tt.gatewayClassName,
						GatewayRef:       tt.gatewayRef,
					},
				}
			} else if tt.expectError {
				upgradeStrategy = &rayv1.RayServiceUpgradeStrategy{
					Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
				}
			}

			rayClusterSpec := *createBasicRayClusterSpec()
			rayClusterSpec.EnableInTreeAutoscaling = new(tt.enableAutoscaling)

			rayService := &rayv1.RayService{
				Spec: rayv1.RayServiceSpec{
					RayClusterSpec:  rayClusterSpec,
					UpgradeStrategy: upgradeStrategy,
				},
			}

			err := ValidateClusterUpgradeOptions(rayService)
			if tt.expectError {
				require.Error(t, err, tt.name)
			} else {
				require.NoError(t, err, tt.name)
			}
		})
	}
}

func TestValidateRayClusterSpec_IdleTimeoutSeconds(t *testing.T) {
	// Util function to create a RayCluster spec.
	createSpec := func() rayv1.RayClusterSpec {
		return rayv1.RayClusterSpec{
			EnableInTreeAutoscaling: new(true),
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: podTemplateSpec(nil, nil),
			},
		}
	}

	tests := map[string]struct {
		expectedErr string
		spec        rayv1.RayClusterSpec
	}{
		// Worker group tests
		"Valid: Worker group with valid idleTimeoutSeconds and v2 spec field": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:          "worker-group-1",
						Template:           podTemplateSpec(nil, nil),
						IdleTimeoutSeconds: new(int32(60)),
						MinReplicas:        new(int32(0)),
						MaxReplicas:        new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: Worker group with idleTimeoutSeconds and v2 env var": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.HeadGroupSpec.Template = podTemplateSpec([]corev1.EnvVar{
					{Name: RAY_ENABLE_AUTOSCALER_V2, Value: "1"},
				}, nil)
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:          "worker-group-1",
						Template:           podTemplateSpec(nil, nil),
						IdleTimeoutSeconds: new(int32(60)),
						MinReplicas:        new(int32(0)),
						MaxReplicas:        new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: Worker group with zero idleTimeoutSeconds": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:          "worker-group-1",
						Template:           podTemplateSpec(nil, nil),
						IdleTimeoutSeconds: new(int32(0)),
						MinReplicas:        new(int32(0)),
						MaxReplicas:        new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "",
		},
		"Invalid: Worker group with negative idleTimeoutSeconds": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:          "worker-group-1",
						Template:           podTemplateSpec(nil, nil),
						IdleTimeoutSeconds: new(int32(-10)),
						MinReplicas:        new(int32(0)),
						MaxReplicas:        new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "worker group worker-group-1: idleTimeoutSeconds must be non-negative, got -10",
		},
		"Invalid: Worker group idleTimeoutSeconds without v2": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:          "worker-group-1",
						Template:           podTemplateSpec(nil, nil),
						IdleTimeoutSeconds: new(int32(60)),
						MinReplicas:        new(int32(0)),
						MaxReplicas:        new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: fmt.Sprintf("worker group worker-group-1: idleTimeoutSeconds is set, but autoscaler v2 is not enabled. Please set .spec.autoscalerOptions.version to 'v2' (or set %s environment variable to 'true' in the head pod if using KubeRay < 1.4.0)", RAY_ENABLE_AUTOSCALER_V2),
		},
		"Invalid: Worker group idleTimeoutSeconds with invalid env var": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.HeadGroupSpec.Template = podTemplateSpec([]corev1.EnvVar{
					{Name: RAY_ENABLE_AUTOSCALER_V2, Value: "false"},
				}, nil)
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:          "worker-group-1",
						Template:           podTemplateSpec(nil, nil),
						IdleTimeoutSeconds: new(int32(60)),
						MinReplicas:        new(int32(0)),
						MaxReplicas:        new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: fmt.Sprintf("worker group worker-group-1: idleTimeoutSeconds is set, but autoscaler v2 is not enabled. Please set .spec.autoscalerOptions.version to 'v2' (or set %s environment variable to 'true' in the head pod if using KubeRay < 1.4.0)", RAY_ENABLE_AUTOSCALER_V2),
		},
		"Valid: Worker group without idleTimeoutSeconds": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-1",
						Template:    podTemplateSpec(nil, nil),
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "",
		},

		// AutoscalerOptions tests (idleTimeoutSeconds works with both v1 and v2)
		"Valid: AutoscalerOptions with idleTimeoutSeconds and v2": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version:            ptr.To(rayv1.AutoscalerVersionV2),
					IdleTimeoutSeconds: new(int32(120)),
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: AutoscalerOptions with idleTimeoutSeconds and v1": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version:            ptr.To(rayv1.AutoscalerVersionV1),
					IdleTimeoutSeconds: new(int32(120)),
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: AutoscalerOptions with idleTimeoutSeconds and no version": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					IdleTimeoutSeconds: new(int32(120)),
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: AutoscalerOptions with zero idleTimeoutSeconds": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					IdleTimeoutSeconds: new(int32(0)),
				}
				return s
			}(),
			expectedErr: "",
		},
		"Invalid: AutoscalerOptions with negative idleTimeoutSeconds": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					IdleTimeoutSeconds: new(int32(-10)),
				}
				return s
			}(),
			expectedErr: "autoscalerOptions.idleTimeoutSeconds must be non-negative, got -10",
		},
		"Valid: AutoscalerOptions without idleTimeoutSeconds": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV1),
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: AutoscalerOptions is nil": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = nil
				return s
			}(),
			expectedErr: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateRayClusterSpec(&tc.spec, nil)
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpec_Priority(t *testing.T) {
	// Util function to create a RayCluster spec.
	createSpec := func() rayv1.RayClusterSpec {
		return rayv1.RayClusterSpec{
			RayVersion:              "2.56.0",
			EnableInTreeAutoscaling: new(true),
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: podTemplateSpec(nil, nil),
			},
		}
	}

	tests := map[string]struct {
		expectedErr string
		spec        rayv1.RayClusterSpec
	}{
		"Valid: Worker group with priority and v2 spec field": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-1",
						Template:    podTemplateSpec(nil, nil),
						Priority:    new(int32(1)),
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: Worker group with priority and v2 env var": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.HeadGroupSpec.Template = podTemplateSpec([]corev1.EnvVar{
					{Name: RAY_ENABLE_AUTOSCALER_V2, Value: "1"},
				}, nil)
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-1",
						Template:    podTemplateSpec(nil, nil),
						Priority:    new(int32(1)),
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: Worker group with zero priority and v2 disabled": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-1",
						Template:    podTemplateSpec(nil, nil),
						Priority:    new(int32(0)),
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "",
		},
		"Valid: Worker group without priority and v2 disabled": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-1",
						Template:    podTemplateSpec(nil, nil),
						Priority:    nil,
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "",
		},
		"Invalid: Worker group priority without v2": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-1",
						Template:    podTemplateSpec(nil, nil),
						Priority:    new(int32(1)),
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "worker group worker-group-1: priority is set to 1, but autoscaler v2 is not enabled. Priority is only supported with autoscaler v2 enabled",
		},
		"Invalid: Worker group priority with invalid env var": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.HeadGroupSpec.Template = podTemplateSpec([]corev1.EnvVar{
					{Name: RAY_ENABLE_AUTOSCALER_V2, Value: "false"},
				}, nil)
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-1",
						Template:    podTemplateSpec(nil, nil),
						Priority:    new(int32(2)),
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "worker group worker-group-1: priority is set to 2, but autoscaler v2 is not enabled. Priority is only supported with autoscaler v2 enabled",
		},
		"Invalid: Worker group priority with empty Ray version": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.RayVersion = ""
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-1",
						Template:    podTemplateSpec(nil, nil),
						Priority:    new(int32(1)),
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "worker group worker-group-1: priority is set, but RayVersion was not specified. Ray version 2.56.0 or later is required",
		},
		"Invalid: Worker group priority with invalid Ray version": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.RayVersion = "invalid"
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-1",
						Template:    podTemplateSpec(nil, nil),
						Priority:    new(int32(1)),
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "worker group worker-group-1: priority is set, but RayVersion format is invalid: invalid, could not parse \"invalid\" as version",
		},
		"Invalid: Worker group priority with Ray version below 2.56.0": {
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.RayVersion = "2.55.0"
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					Version: ptr.To(rayv1.AutoscalerVersionV2),
				}
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-1",
						Template:    podTemplateSpec(nil, nil),
						Priority:    new(int32(1)),
						MinReplicas: new(int32(0)),
						MaxReplicas: new(int32(10)),
					},
				}
				return s
			}(),
			expectedErr: "worker group worker-group-1: priority is set, but minimum Ray version is 2.56.0, got 2.55.0",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := ValidateRayClusterSpec(&tc.spec, nil)
			if tc.expectedErr != "" {
				require.Error(t, err)
				require.EqualError(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayCronJobSpec(t *testing.T) {
	tests := []struct {
		cronJob     *rayv1.RayCronJob
		name        string
		errorMsg    string
		expectError bool
	}{
		{
			name: "Valid cron schedule and RayJob spec",
			cronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Spec: rayv1.RayCronJobSpec{
					Schedule: "*/5 * * * *",
					JobTemplate: rayv1.RayJobSpec{
						Entrypoint: "python test.py",
						RayClusterSpec: &rayv1.RayClusterSpec{
							HeadGroupSpec: rayv1.HeadGroupSpec{
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{
											{
												Name:  "ray-head",
												Image: "rayproject/ray:2.52.0",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "Invalid cron schedule",
			cronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Spec: rayv1.RayCronJobSpec{
					Schedule: "invalid cron",
					JobTemplate: rayv1.RayJobSpec{
						Entrypoint: "python test.py",
						RayClusterSpec: &rayv1.RayClusterSpec{
							HeadGroupSpec: rayv1.HeadGroupSpec{
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{
											{
												Name:  "ray-head",
												Image: "rayproject/ray:2.52.0",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "invalid cron schedule",
		},
		{
			name: "Empty JobTemplate",
			cronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Spec: rayv1.RayCronJobSpec{
					Schedule:    "*/5 * * * *",
					JobTemplate: rayv1.RayJobSpec{},
				},
			},
			expectError: true,
			// We use ValidateRayJobSpec() for validating JobTemplate, which requires either RayClusterSpec or ClusterSelector to be set
			errorMsg: "one of RayClusterSpec or ClusterSelector must be set",
		},
		{
			name: "Invalid RayJob spec - no containers",
			cronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Spec: rayv1.RayCronJobSpec{
					Schedule: "*/5 * * * *",
					JobTemplate: rayv1.RayJobSpec{
						Entrypoint: "python test.py",
						RayClusterSpec: &rayv1.RayClusterSpec{
							HeadGroupSpec: rayv1.HeadGroupSpec{
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "invalid RayJob template",
		},
		{
			name: "Valid schedule with valid timezone",
			cronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Spec: rayv1.RayCronJobSpec{
					Schedule: "0 9 * * *",
					TimeZone: new("Asia/Taipei"),
					JobTemplate: rayv1.RayJobSpec{
						Entrypoint: "python test.py",
						RayClusterSpec: &rayv1.RayClusterSpec{
							HeadGroupSpec: rayv1.HeadGroupSpec{
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{
											{
												Name:  "ray-head",
												Image: "rayproject/ray:2.52.0",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "Valid schedule with UTC timezone",
			cronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Spec: rayv1.RayCronJobSpec{
					Schedule: "0 0 * * *",
					TimeZone: new("UTC"),
					JobTemplate: rayv1.RayJobSpec{
						Entrypoint: "python test.py",
						RayClusterSpec: &rayv1.RayClusterSpec{
							HeadGroupSpec: rayv1.HeadGroupSpec{
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{
											{
												Name:  "ray-head",
												Image: "rayproject/ray:2.52.0",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: false,
		},
		{
			name: "Invalid timezone",
			cronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Spec: rayv1.RayCronJobSpec{
					Schedule: "*/5 * * * *",
					TimeZone: new("Invalid/Zone"),
					JobTemplate: rayv1.RayJobSpec{
						Entrypoint: "python test.py",
						RayClusterSpec: &rayv1.RayClusterSpec{
							HeadGroupSpec: rayv1.HeadGroupSpec{
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{
											{
												Name:  "ray-head",
												Image: "rayproject/ray:2.52.0",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "invalid time zone",
		},
		{
			name: "Schedule with TZ= prefix is rejected",
			cronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Spec: rayv1.RayCronJobSpec{
					Schedule: "TZ=UTC */5 * * * *",
					JobTemplate: rayv1.RayJobSpec{
						Entrypoint: "python test.py",
						RayClusterSpec: &rayv1.RayClusterSpec{
							HeadGroupSpec: rayv1.HeadGroupSpec{
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{
											{
												Name:  "ray-head",
												Image: "rayproject/ray:2.52.0",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "cannot use TZ or CRON_TZ in schedule",
		},
		{
			name: "Schedule with CRON_TZ= prefix is rejected",
			cronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cronjob",
					Namespace: "default",
				},
				Spec: rayv1.RayCronJobSpec{
					Schedule: "CRON_TZ=UTC */5 * * * *",
					JobTemplate: rayv1.RayJobSpec{
						Entrypoint: "python test.py",
						RayClusterSpec: &rayv1.RayClusterSpec{
							HeadGroupSpec: rayv1.HeadGroupSpec{
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{
											{
												Name:  "ray-head",
												Image: "rayproject/ray:2.52.0",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "cannot use TZ or CRON_TZ in schedule",
		},
		{
			name: "RayCronJob name too long",
			cronJob: &rayv1.RayCronJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      strings.Repeat("a", MaxRayCronJobNameLength+1),
					Namespace: "default",
				},
				Spec: rayv1.RayCronJobSpec{
					Schedule: "*/5 * * * *",
					JobTemplate: rayv1.RayJobSpec{
						Entrypoint: "python test.py",
						RayClusterSpec: &rayv1.RayClusterSpec{
							HeadGroupSpec: rayv1.HeadGroupSpec{
								Template: corev1.PodTemplateSpec{
									Spec: corev1.PodSpec{
										Containers: []corev1.Container{
											{
												Name:  "ray-head",
												Image: "rayproject/ray:2.9.0",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			expectError: true,
			errorMsg:    "RayCronJob name should be no more than",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateRayCronJobSpec(tc.cronJob)

			if tc.expectError {
				require.Error(t, err)
				if tc.errorMsg != "" {
					assert.Contains(t, err.Error(), tc.errorMsg)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterUpgradeOptions(t *testing.T) {
	tests := []struct {
		upgradeStrategy   *rayv1.RayClusterUpgradeStrategy
		name              string
		originatedFromCRD string
		errorMessage      string
		expectError       bool
	}{
		{
			name:            "No upgrade strategy",
			upgradeStrategy: nil,
			expectError:     false,
		},
		{
			name: "Upgrade strategy set None for RayCluster",
			upgradeStrategy: &rayv1.RayClusterUpgradeStrategy{
				Type: ptr.To(rayv1.RayClusterUpgradeNone),
			},
			originatedFromCRD: string(RayClusterCRD),
			expectError:       false,
		},
		{
			name: "Upgrade strategy set Recreate for RayCluster",
			upgradeStrategy: &rayv1.RayClusterUpgradeStrategy{
				Type: ptr.To(rayv1.RayClusterRecreate),
			},
			originatedFromCRD: string(RayClusterCRD),
			expectError:       false,
		},
		{
			name: "Upgrade strategy set Recreate for RayCluster created by RayJob",
			upgradeStrategy: &rayv1.RayClusterUpgradeStrategy{
				Type: ptr.To(rayv1.RayClusterRecreate),
			},
			originatedFromCRD: string(RayJobCRD),
			expectError:       true,
			errorMessage:      "upgradeStrategy cannot be set when RayCluster is created by RayJob",
		},
		{
			name: "Upgrade strategy set Recreate for RayCluster created by RayService",
			upgradeStrategy: &rayv1.RayClusterUpgradeStrategy{
				Type: ptr.To(rayv1.RayClusterRecreate),
			},
			originatedFromCRD: string(RayServiceCRD),
			expectError:       true,
			errorMessage:      "upgradeStrategy cannot be set when RayCluster is created by RayService",
		},
		{
			name: "Invalid upgrade strategy value",
			upgradeStrategy: &rayv1.RayClusterUpgradeStrategy{
				Type: ptr.To(rayv1.RayClusterUpgradeType("InvalidStrategy")),
			},
			originatedFromCRD: string(RayClusterCRD),
			expectError:       true,
			errorMessage:      "Spec.UpgradeStrategy.Type value InvalidStrategy is invalid, valid options are Recreate or None",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: "default",
					Labels:    map[string]string{},
				},
				Spec: rayv1.RayClusterSpec{
					UpgradeStrategy: tt.upgradeStrategy,
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: corev1.PodTemplateSpec{
							Spec: corev1.PodSpec{
								Containers: []corev1.Container{
									{Name: "ray-head", Image: "rayproject/ray:latest"},
								},
							},
						},
					},
				},
			}

			if tt.originatedFromCRD != "" {
				cluster.Labels[RayOriginatedFromCRDLabelKey] = tt.originatedFromCRD
			}

			err := ValidateRayClusterUpgradeOptions(cluster)

			if tt.expectError {
				require.Error(t, err, "Expected error for test case: %s", tt.name)
				if tt.errorMessage != "" {
					assert.Contains(t, err.Error(), tt.errorMessage, "Error message mismatch for test case: %s", tt.name)
				}
			} else {
				require.NoError(t, err, "Unexpected error for test case: %s", tt.name)
			}
		})
	}
}

func TestValidateRayClusterSpec_WorkerGroupReplicaValidation(t *testing.T) {
	createSpec := func() rayv1.RayClusterSpec {
		return rayv1.RayClusterSpec{
			EnableInTreeAutoscaling: new(false),
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: podTemplateSpec(nil, nil),
			},
		}
	}

	tests := []struct {
		name        string
		errorMsg    string
		spec        rayv1.RayClusterSpec
		expectError bool
	}{
		{
			name: "minReplicas greater than maxReplicas",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-3",
						Template:    podTemplateSpec(nil, nil),
						MinReplicas: new(int32(5)),
						MaxReplicas: new(int32(3)),
					},
				}
				return s
			}(),
			expectError: true,
			errorMsg:    "worker group worker-group-3 has minReplicas 5 greater than maxReplicas 3",
		},
		{
			// This test always passes since we have clamped the replicas to minReplicas in GetWorkerGroupDesiredReplicas.
			name: "replicas smaller than minReplicas when autoscaling disabled",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-3",
						Template:    podTemplateSpec(nil, nil),
						Replicas:    new(int32(1)),
						MinReplicas: new(int32(2)),
						MaxReplicas: new(int32(5)),
					},
				}
				return s
			}(),
			expectError: false,
		},
		{
			name: "minReplicas are nil when autoscaling disabled",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-3",
						Template:    podTemplateSpec(nil, nil),
						MinReplicas: nil,
						MaxReplicas: new(int32(5)),
					},
				}
				return s
			}(),
			expectError: true,
			errorMsg:    "worker group worker-group-3 must set both minReplicas and maxReplicas when autoscaling is disabled",
		},
		{
			name: "maxReplicas are nil when autoscaling disabled",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-3",
						Template:    podTemplateSpec(nil, nil),
						MinReplicas: new(int32(1)),
						MaxReplicas: nil,
					},
				}
				return s
			}(),
			expectError: true,
			errorMsg:    "worker group worker-group-3 must set both minReplicas and maxReplicas when autoscaling is disabled",
		},
		{
			name: "minReplicas and maxReplicas both are nil when autoscaling disabled",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-3",
						Template:    podTemplateSpec(nil, nil),
						MinReplicas: nil,
						MaxReplicas: nil,
					},
				}
				return s
			}(),
			expectError: true,
			errorMsg:    "worker group worker-group-3 must set both minReplicas and maxReplicas when autoscaling is disabled",
		},
		{
			name: "minReplicas is negative",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.EnableInTreeAutoscaling = new(true)
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-3",
						Template:    podTemplateSpec(nil, nil),
						MinReplicas: new(int32(-1)),
					},
				}
				return s
			}(),
			expectError: true,
			errorMsg:    "worker group worker-group-3 has negative minReplicas -1",
		},
		{
			name: "maxReplicas is negative",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.EnableInTreeAutoscaling = new(true)
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName:   "worker-group-3",
						Template:    podTemplateSpec(nil, nil),
						MaxReplicas: new(int32(-1)),
					},
				}
				return s
			}(),
			expectError: true,
			errorMsg:    "worker group worker-group-3 has negative maxReplicas -1",
		},
		{
			name: "valid when autoscaling enabled",
			spec: func() rayv1.RayClusterSpec {
				s := createSpec()
				s.EnableInTreeAutoscaling = new(true)
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{
					{
						GroupName: "worker-group-3",
						Template:  podTemplateSpec(nil, nil),
					},
				}
				return s
			}(),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRayClusterSpec(&tt.spec, nil)
			if tt.expectError {
				require.Error(t, err)
				require.EqualError(t, err, tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpec_Auth(t *testing.T) {
	tests := []struct {
		authOptions *rayv1.AuthOptions
		name        string
		errorMsg    string
		expectError bool
	}{
		{
			name: "enableK8sTokenAuth=true and secretName set",
			authOptions: &rayv1.AuthOptions{
				Mode:               rayv1.AuthModeToken,
				EnableK8sTokenAuth: new(true),
				SecretName:         new("my-secret"),
			},
			expectError: true,
			errorMsg:    "authOptions.enableK8sTokenAuth is enabled and authOptions.secretName is also set",
		},
		{
			name: "enableK8sTokenAuth=true and secretName unset",
			authOptions: &rayv1.AuthOptions{
				Mode:               rayv1.AuthModeToken,
				EnableK8sTokenAuth: new(true),
			},
			expectError: false,
		},
		{
			name: "enableK8sTokenAuth=false and secretName set",
			authOptions: &rayv1.AuthOptions{
				Mode:       rayv1.AuthModeToken,
				SecretName: new("my-secret"),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					RayVersion:  "2.55.0", // Required for checks
					AuthOptions: tt.authOptions,
					HeadGroupSpec: rayv1.HeadGroupSpec{
						Template: podTemplateSpec(nil, nil),
					},
					WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
						{
							GroupName:   "worker-group",
							Template:    podTemplateSpec(nil, nil),
							MinReplicas: new(int32(1)),
							MaxReplicas: new(int32(1)),
						},
					},
				},
			}
			err := ValidateRayClusterSpec(&cluster.Spec, cluster.Annotations)
			if tt.expectError {
				require.Error(t, err)
				if tt.errorMsg != "" {
					assert.Contains(t, err.Error(), tt.errorMsg)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateNetworkPolicy(t *testing.T) {
	tests := []struct {
		ni               *rayv1.NetworkPolicyConfig
		name             string
		errorMsg         string
		workerGroupSpecs []rayv1.WorkerGroupSpec
		expectError      bool
	}{
		{
			name: "DenyAllEgress with head IngressRules set returns error",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAllEgress),
				Head: &rayv1.NetworkPolicyRules{
					IngressRules: []networkingv1.NetworkPolicyIngressRule{{}},
				},
			},
			expectError: true,
			errorMsg:    `networkPolicy.head.ingressRules cannot be set when mode is "DenyAllEgress" (ingress is not restricted)`,
		},
		{
			name: "DenyAllEgress with worker IngressRules set returns error",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAllEgress),
				Worker: &rayv1.NetworkPolicyRules{
					IngressRules: []networkingv1.NetworkPolicyIngressRule{{}},
				},
			},
			expectError: true,
			errorMsg:    `networkPolicy.worker.ingressRules cannot be set when mode is "DenyAllEgress" (ingress is not restricted)`,
		},
		{
			name: "DenyAllIngress with head EgressRules set returns error",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAllIngress),
				Head: &rayv1.NetworkPolicyRules{
					EgressRules: []networkingv1.NetworkPolicyEgressRule{{}},
				},
			},
			expectError: true,
			errorMsg:    `networkPolicy.head.egressRules cannot be set when mode is "DenyAllIngress" (egress is not restricted)`,
		},
		{
			name: "DenyAllIngress with worker EgressRules set returns error",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAllIngress),
				Worker: &rayv1.NetworkPolicyRules{
					EgressRules: []networkingv1.NetworkPolicyEgressRule{{}},
				},
			},
			expectError: true,
			errorMsg:    `networkPolicy.worker.egressRules cannot be set when mode is "DenyAllIngress" (egress is not restricted)`,
		},
		{
			name: "DenyAll with both head and worker rules is valid",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAll),
				Head: &rayv1.NetworkPolicyRules{
					IngressRules: []networkingv1.NetworkPolicyIngressRule{{}},
					EgressRules:  []networkingv1.NetworkPolicyEgressRule{{}},
				},
				Worker: &rayv1.NetworkPolicyRules{
					IngressRules: []networkingv1.NetworkPolicyIngressRule{{}},
					EgressRules:  []networkingv1.NetworkPolicyEgressRule{{}},
				},
			},
			expectError: false,
		},
		{
			name: "DenyAllEgress with worker group IngressRules set returns error",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAllEgress),
				WorkerGroups: []rayv1.WorkerGroupNetworkPolicyRules{
					{
						GroupName: "small-group",
						NetworkPolicyRules: rayv1.NetworkPolicyRules{
							IngressRules: []networkingv1.NetworkPolicyIngressRule{{}},
						},
					},
				},
			},
			workerGroupSpecs: []rayv1.WorkerGroupSpec{{GroupName: "small-group"}},
			expectError:      true,
			errorMsg:         `networkPolicy.workerGroups["small-group"].ingressRules cannot be set when mode is "DenyAllEgress" (ingress is not restricted)`,
		},
		{
			name: "DenyAllIngress with worker group EgressRules set returns error",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAllIngress),
				WorkerGroups: []rayv1.WorkerGroupNetworkPolicyRules{
					{
						GroupName: "small-group",
						NetworkPolicyRules: rayv1.NetworkPolicyRules{
							EgressRules: []networkingv1.NetworkPolicyEgressRule{{}},
						},
					},
				},
			},
			workerGroupSpecs: []rayv1.WorkerGroupSpec{{GroupName: "small-group"}},
			expectError:      true,
			errorMsg:         `networkPolicy.workerGroups["small-group"].egressRules cannot be set when mode is "DenyAllIngress" (egress is not restricted)`,
		},
		{
			name: "worker group entry not matching any workerGroupSpecs returns error",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAll),
				WorkerGroups: []rayv1.WorkerGroupNetworkPolicyRules{
					{
						GroupName: "samll-group",
						NetworkPolicyRules: rayv1.NetworkPolicyRules{
							IngressRules: []networkingv1.NetworkPolicyIngressRule{{}},
						},
					},
				},
			},
			workerGroupSpecs: []rayv1.WorkerGroupSpec{{GroupName: "small-group"}},
			expectError:      true,
			errorMsg:         `networkPolicy.workerGroups["samll-group"] does not match any group name in workerGroupSpecs`,
		},
		{
			name: "worker group entry matching workerGroupSpecs is valid",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAll),
				WorkerGroups: []rayv1.WorkerGroupNetworkPolicyRules{
					{
						GroupName: "small-group",
						NetworkPolicyRules: rayv1.NetworkPolicyRules{
							IngressRules: []networkingv1.NetworkPolicyIngressRule{{}},
							EgressRules:  []networkingv1.NetworkPolicyEgressRule{{}},
						},
					},
				},
			},
			workerGroupSpecs: []rayv1.WorkerGroupSpec{{GroupName: "small-group"}},
			expectError:      false,
		},
		{
			name: "worker group name that is not a valid DNS1123 label returns error",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAll),
			},
			workerGroupSpecs: []rayv1.WorkerGroupSpec{{GroupName: "GPU-Group"}},
			expectError:      true,
			errorMsg:         `worker group name "GPU-Group" must be a valid DNS1123 label when networkPolicy is enabled`,
		},
		{
			name:        "nil NetworkPolicy is valid",
			ni:          nil,
			expectError: false,
		},
		{
			name: "mode only with no head or worker rules is valid",
			ni: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAll),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := &rayv1.RayClusterSpec{NetworkPolicy: tt.ni, WorkerGroupSpecs: tt.workerGroupSpecs}
			err := validateNetworkPolicy(spec)
			if tt.expectError {
				require.Error(t, err)
				assert.ErrorContains(t, err, tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpec_NetworkPolicyRequiresFeatureGate(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterNetworkPolicy, false)
	cluster := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			NetworkPolicy: &rayv1.NetworkPolicyConfig{
				Mode: ptr.To(rayv1.NetworkPolicyDenyAll),
			},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: podTemplateSpec(nil, nil),
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:   "worker-group",
					Template:    podTemplateSpec(nil, nil),
					MinReplicas: new(int32(1)),
					MaxReplicas: new(int32(1)),
				},
			},
		},
	}
	err := ValidateRayClusterSpec(&cluster.Spec, cluster.Annotations)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RayClusterNetworkPolicy")
}

func TestValidateTLSOptions(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, true)
	baseSpec := func() rayv1.RayClusterSpec {
		return rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{{Name: "ray-head", Image: "rayproject/ray:latest"}},
					},
				},
			},
		}
	}

	tests := []struct {
		modify      func(*rayv1.RayClusterSpec)
		name        string
		errorMsg    string
		expectError bool
	}{
		{
			name: "TLS disabled, no options - valid",
			modify: func(_ *rayv1.RayClusterSpec) {
			},
		},
		{
			name: "TLS enabled, no options (auto-generate) - valid",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
			},
		},
		{
			name: "tlsOptions set but enabled is false - valid (disables TLS)",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(false)}
			},
		},
		{
			name: "RAY_USE_TLS in head container - error",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
				s.HeadGroupSpec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "RAY_USE_TLS", Value: "1"}}
			},
			expectError: true,
			errorMsg:    "cannot set RAY_USE_TLS",
		},
		{
			name: "RAY_TLS_SERVER_CERT in head container - error",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
				s.HeadGroupSpec.Template.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "RAY_TLS_SERVER_CERT", Value: "/x"}}
			},
			expectError: true,
			errorMsg:    "cannot set RAY_TLS_SERVER_CERT",
		},
		{
			name: "RAY_TLS_SERVER_KEY in worker container - error",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{{
					GroupName: "wg",
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "ray-worker",
								Image: "rayproject/ray:latest",
								Env:   []corev1.EnvVar{{Name: "RAY_TLS_SERVER_KEY", Value: "/x"}},
							}},
						},
					},
				}}
			},
			expectError: true,
			errorMsg:    "cannot set RAY_TLS_SERVER_KEY",
		},
		{
			name: "RAY_TLS_CA_CERT in worker container - error",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{{
					GroupName: "wg",
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "ray-worker",
								Image: "rayproject/ray:latest",
								Env:   []corev1.EnvVar{{Name: "RAY_TLS_CA_CERT", Value: "/x"}},
							}},
						},
					},
				}}
			},
			expectError: true,
			errorMsg:    "cannot set RAY_TLS_CA_CERT",
		},
		{
			name: "conflicting TLS volume name in head container - error",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
				s.HeadGroupSpec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
					{Name: RayTLSVolumeName, MountPath: "/some/path"},
				}
			},
			expectError: true,
			errorMsg:    "cannot use volume mount named",
		},
		{
			name: "conflicting TLS mount path in head container - error",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
				s.HeadGroupSpec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
					{Name: "custom-tls", MountPath: RayTLSCertMountPath},
				}
			},
			expectError: true,
			errorMsg:    "cannot use volume mount at path",
		},
		{
			name: "conflicting TLS volume name in worker container - error",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
				s.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{{
					GroupName: "wg",
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{{
								Name:  "ray-worker",
								Image: "rayproject/ray:latest",
								VolumeMounts: []corev1.VolumeMount{
									{Name: RayTLSVolumeName, MountPath: "/some/path"},
								},
							}},
						},
					},
				}}
			},
			expectError: true,
			errorMsg:    "cannot use volume mount named",
		},
		{
			name: "non-conflicting volume mount in head container - valid",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
				s.HeadGroupSpec.Template.Spec.Containers[0].VolumeMounts = []corev1.VolumeMount{
					{Name: "user-volume", MountPath: "/user/path"},
				}
			},
			expectError: false,
		},
		{
			name: "conflicting TLS volume name in autoscalerOptions - error",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					VolumeMounts: []corev1.VolumeMount{
						{Name: RayTLSVolumeName, MountPath: "/some/path"},
					},
				}
			},
			expectError: true,
			errorMsg:    "cannot use volume mount named",
		},
		{
			name: "conflicting TLS mount path in autoscalerOptions - error",
			modify: func(s *rayv1.RayClusterSpec) {
				s.TLSOptions = &rayv1.TLSOptions{Enabled: new(true)}
				s.AutoscalerOptions = &rayv1.AutoscalerOptions{
					VolumeMounts: []corev1.VolumeMount{
						{Name: "custom-tls", MountPath: RayTLSCertMountPath},
					},
				}
			},
			expectError: true,
			errorMsg:    "cannot use volume mount at path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := baseSpec()
			tt.modify(&spec)
			err := validateTLSOptions(&spec)
			if tt.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errorMsg)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateRayClusterSpec_TLSOptionsRequiresFeatureGate(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterMTLS, false)
	cluster := &rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			TLSOptions: &rayv1.TLSOptions{Enabled: new(true)},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: podTemplateSpec(nil, nil),
			},
		},
	}
	err := ValidateRayClusterSpec(&cluster.Spec, cluster.Annotations)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RayClusterMTLS")
}

func TestValidateRayClusterSpec_HistoryServerRequiresFeatureGate(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterHistoryServer, false)
	cluster := &rayv1.RayCluster{
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
				Template: podTemplateSpec(nil, nil),
			},
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:   "worker-group",
					Template:    podTemplateSpec(nil, nil),
					MinReplicas: new(int32(1)),
					MaxReplicas: new(int32(1)),
				},
			},
		},
	}
	err := ValidateRayClusterSpec(&cluster.Spec, cluster.Annotations)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RayClusterHistoryServer feature gate is not enabled")
}

func TestValidateCollectorOptions(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterHistoryServer, true)

	tests := []struct {
		name         string
		collector    *rayv1.CollectorOptions
		headSpec     *rayv1.HeadGroupSpec
		workerSpecs  []rayv1.WorkerGroupSpec
		expectError  bool
		errorMessage string
	}{
		{
			name:         "nil collector options",
			collector:    nil,
			expectError:  true,
			errorMessage: "historyServerOptions.collectorOptions must be set",
		},
		{
			name: "missing STORAGE_BACKEND",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
			},
			expectError:  true,
			errorMessage: "STORAGE_BACKEND environment variable must be set with a literal string value in historyServerOptions.collectorOptions.env",
		},
		{
			name: "valid GCS",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "GCS"},
					{Name: "GCS_BUCKET", Value: "my-bucket"},
				},
			},
		},
		{
			name: "valid GCS with ValueFrom secret",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "GCS"},
					{
						Name: "GCS_BUCKET",
						ValueFrom: &corev1.EnvVarSource{
							SecretKeyRef: &corev1.SecretKeySelector{
								LocalObjectReference: corev1.LocalObjectReference{Name: "my-gcs-secret"},
								Key:                  "bucket",
							},
						},
					},
				},
			},
		},
		{
			name: "missing GCS_BUCKET",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "GCS"},
				},
			},
			expectError:  true,
			errorMessage: "GCS_BUCKET environment variable must be set when STORAGE_BACKEND is GCS",
		},
		{
			name: "valid S3 with all fields",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "S3"},
					{Name: "S3_BUCKET", Value: "my-bucket"},
					{Name: "S3_ENDPOINT", Value: "s3.us-west-2.amazonaws.com"},
					{Name: "S3_REGION", Value: "us-west-2"},
				},
			},
		},
		{
			name: "missing S3_REGION",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "S3"},
					{Name: "S3_BUCKET", Value: "my-bucket"},
					{Name: "S3_ENDPOINT", Value: "s3.us-west-2.amazonaws.com"},
				},
			},
			expectError:  true,
			errorMessage: "S3_REGION environment variable must be set when STORAGE_BACKEND is S3",
		},
		{
			name: "valid AZUREBLOB with connection string mode",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "AZUREBLOB"},
					{Name: "AZURE_STORAGE_AUTH_MODE", Value: "connection_string"},
					{Name: "AZURE_STORAGE_CONNECTION_STRING", Value: "connection-string"},
				},
			},
		},
		{
			name: "valid AZUREBLOB with workload identity mode",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "AZUREBLOB"},
					{Name: "AZURE_STORAGE_AUTH_MODE", Value: "workload_identity"},
					{Name: "AZURE_STORAGE_ACCOUNT_URL", Value: "https://myaccount.blob.core.windows.net"},
				},
			},
		},
		{
			name: "missing connection string in connection_string auth mode",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "AZUREBLOB"},
					{Name: "AZURE_STORAGE_AUTH_MODE", Value: "connection_string"},
				},
			},
			expectError:  true,
			errorMessage: "AZURE_STORAGE_CONNECTION_STRING environment variable must be set when STORAGE_BACKEND is AZUREBLOB and AZURE_STORAGE_AUTH_MODE is connection_string",
		},
		{
			name: "missing credentials in auto-detect auth mode",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "AZUREBLOB"},
				},
			},
			expectError:  true,
			errorMessage: "either AZURE_STORAGE_CONNECTION_STRING or AZURE_STORAGE_ACCOUNT_URL environment variable must be set when STORAGE_BACKEND is AZUREBLOB",
		},
		{
			name: "valid ALIYUNOSS",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "ALIYUNOSS"},
					{Name: "OSS_BUCKET", Value: "my-bucket"},
					{Name: "OSS_ENDPOINT", Value: "oss-cn-hangzhou.aliyuncs.com"},
					{Name: "OSS_REGION", Value: "cn-hangzhou"},
				},
			},
		},
		{
			name: "missing OSS_REGION",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "ALIYUNOSS"},
					{Name: "OSS_BUCKET", Value: "my-bucket"},
					{Name: "OSS_ENDPOINT", Value: "oss-cn-hangzhou.aliyuncs.com"},
				},
			},
			expectError:  true,
			errorMessage: "OSS_REGION environment variable must be set when STORAGE_BACKEND is ALIYUNOSS",
		},
		{
			name: "unknown storage backend (skips validation)",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "CUSTOM_BACKEND"},
				},
			},
			expectError: false,
		},
		{
			name: "manually defined ray-history-collector container in head template",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "GCS"},
					{Name: "GCS_BUCKET", Value: "my-bucket"},
				},
			},
			headSpec: &rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: CollectorContainerName, Image: "user-image"},
						},
					},
				},
			},
			expectError:  true,
			errorMessage: fmt.Sprintf("head pod template must not define a container with name '%s' when history server collector options are enabled", CollectorContainerName),
		},
		{
			name: "manually defined ray-history-collector container in worker template",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "GCS"},
					{Name: "GCS_BUCKET", Value: "my-bucket"},
				},
			},
			workerSpecs: []rayv1.WorkerGroupSpec{
				{
					GroupName:   "group-1",
					MinReplicas: new(int32(1)),
					MaxReplicas: new(int32(1)),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{Name: CollectorContainerName, Image: "user-image"},
							},
						},
					},
				},
			},
			expectError:  true,
			errorMessage: fmt.Sprintf("worker group group-1 pod template must not define a container with name '%s' when history server collector options are enabled", CollectorContainerName),
		},
		{
			name: "manually defined POD_IP in collector env",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "GCS"},
					{Name: "GCS_BUCKET", Value: "my-bucket"},
					{Name: "POD_IP", Value: "10.244.0.5"},
				},
			},
			expectError:  true,
			errorMessage: "historyServerOptions.collectorOptions.env must not contain POD_IP: it is managed by KubeRay and injected automatically into the collector container",
		},
		{
			name: "manually defined EVENTS_PORT in collector env",
			collector: &rayv1.CollectorOptions{
				Image: new("quay.io/kuberay/collector:latest"),
				Env: []corev1.EnvVar{
					{Name: "STORAGE_BACKEND", Value: "GCS"},
					{Name: "GCS_BUCKET", Value: "my-bucket"},
					{Name: "EVENTS_PORT", Value: "9000"},
				},
			},
			expectError:  true,
			errorMessage: "historyServerOptions.collectorOptions.env must not contain EVENTS_PORT: it is managed by KubeRay and injected automatically into the collector container",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var headGroupSpec rayv1.HeadGroupSpec
			if tt.headSpec != nil {
				headGroupSpec = *tt.headSpec
			} else {
				headGroupSpec = rayv1.HeadGroupSpec{
					Template: podTemplateSpec(nil, nil),
				}
			}

			cluster := &rayv1.RayCluster{
				Spec: rayv1.RayClusterSpec{
					HistoryServerOptions: &rayv1.HistoryServerOptions{
						CollectorOptions: tt.collector,
					},
					HeadGroupSpec:    headGroupSpec,
					WorkerGroupSpecs: tt.workerSpecs,
				},
			}
			err := ValidateRayClusterSpec(&cluster.Spec, cluster.Annotations)
			if tt.expectError {
				require.Error(t, err)
				assert.EqualError(t, err, tt.errorMessage)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateRayClusterSpec_LabelRefs checks the webhook and Ray version requirements of a group with labelRefs
func TestValidateRayClusterSpec_LabelRefs(t *testing.T) {
	spec := createBasicRayClusterSpec()
	spec.WorkerGroupSpecs = []rayv1.WorkerGroupSpec{{
		GroupName:   "test",
		MinReplicas: new(int32(1)),
		MaxReplicas: new(int32(1)),
		Template:    corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ray-worker"}}}},
		LabelRefs:   []rayv1.LabelRef{{ValueFrom: rayv1.LabelRefSource{NodeRef: rayv1.NodeFieldRef{FieldPath: "metadata.labels['topology.kubernetes.io/zone']"}}}},
	}}

	t.Setenv("ENABLE_WEBHOOKS", "")
	require.ErrorContains(t, ValidateRayClusterSpec(spec, nil), "requires the NodeLabelDelivery feature gate")

	features.SetFeatureGateDuringTest(t, features.NodeLabelDelivery, true)
	require.ErrorContains(t, ValidateRayClusterSpec(spec, nil), "requires the KubeRay operator to run with ENABLE_WEBHOOKS=true")

	t.Setenv("ENABLE_WEBHOOKS", "true")
	spec.RayVersion = ""
	require.ErrorContains(t, ValidateRayClusterSpec(spec, nil), "is unset or invalid")
	spec.RayVersion = "2.44.0"
	require.ErrorContains(t, ValidateRayClusterSpec(spec, nil), "minimum Ray version is 2.45.0")
	spec.RayVersion = "2.45.0"
	require.NoError(t, ValidateRayClusterSpec(spec, nil))
}
