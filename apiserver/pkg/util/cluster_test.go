package util

import (
	"encoding/json"
	"reflect"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/ray-project/kuberay/proto/go_client"
)

var sizelimit = resource.MustParse("100Gi")

var testVolume = &api.Volume{
	Name:       "hdfs",
	VolumeType: api.Volume_HOST_PATH,
	Source:     "/opt/hdfs",
	MountPath:  "/mnt/hdfs",
	ReadOnly:   true,
}

// There is only an fake case for test both MountPropagationMode and file type
// in real case hostToContainer mode may only valid for directory
var testFileVolume = &api.Volume{
	Name:                 "test-file",
	VolumeType:           api.Volume_HOST_PATH,
	MountPropagationMode: api.Volume_HOSTTOCONTAINER,
	Source:               "/root/proc/stat",
	MountPath:            "/proc/stat",
	HostPathType:         api.Volume_FILE,
	ReadOnly:             true,
}

var testPVCVolume = &api.Volume{
	Name:       "test-pvc",
	VolumeType: api.Volume_PERSISTENT_VOLUME_CLAIM,
	Source:     "my-pvc",
	MountPath:  "/pvc/dir",
	ReadOnly:   true,
}

var testEphemeralVolume = &api.Volume{
	Name:       "test-ephemeral",
	VolumeType: api.Volume_EPHEMERAL,
	MountPath:  "/ephimeral/dir",
	Storage:    "10Gi",
}

var testConfigMapVolume = &api.Volume{
	Name:       "configMap",
	MountPath:  "/tmp/configmap",
	VolumeType: api.Volume_CONFIGMAP,
	Source:     "my-config-map",
	Items: map[string]string{
		"key1": "path1",
		"key2": "path2",
	},
}

var testSecretVolume = &api.Volume{
	Name:       "secret",
	MountPath:  "/tmp/secret",
	VolumeType: api.Volume_SECRET,
	Source:     "my-secret",
}

var testEmptyDirVolume = &api.Volume{
	Name:       "emptyDir",
	MountPath:  "/tmp/emptydir",
	VolumeType: api.Volume_EMPTY_DIR,
	Storage:    "100Gi",
}

var testAutoscalerOptions = api.AutoscalerOptions{
	IdleTimeoutSeconds: 25,
	UpscalingMode:      "Default",
	ImagePullPolicy:    "Always",
	Envs: &api.EnvironmentVariables{
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
		},
	},
	Volumes: []*api.Volume{
		{
			Name:       "configMap",
			MountPath:  "/tmp/configmap",
			VolumeType: api.Volume_CONFIGMAP,
			Source:     "my-config-map",
			Items: map[string]string{
				"key1": "path1",
				"key2": "path2",
			},
		},
		{
			Name:       "secret",
			MountPath:  "/tmp/secret",
			VolumeType: api.Volume_SECRET,
			Source:     "my-secret",
		},
	},
	Cpu: "300m",
}

// Spec for testing
var headGroup = api.HeadGroupSpec{
	ComputeTemplate: "foo",
	Image:           "bar",
	ImagePullPolicy: "Always",
	ServiceType:     "ClusterIP",
	RayStartParams: map[string]string{
		"dashboard-host":      "0.0.0.0",
		"metrics-export-port": "8080",
		"num-cpus":            "0",
	},
	ServiceAccount:  "account",
	ImagePullSecret: "foo",
	EnableIngress:   true,
	Environment: &api.EnvironmentVariables{
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
	},
	Annotations: map[string]string{
		"foo": "bar",
	},
	Labels: map[string]string{
		"foo": "bar",
	},
	SecurityContext: &api.SecurityContext{
		Capabilities: &api.Capabilities{
			Add: []string{"SYS_PTRACE"},
		},
	},
}

var workerGroup = api.WorkerGroupSpec{
	GroupName:       "wg",
	ComputeTemplate: "foo",
	Image:           "bar",
	ImagePullPolicy: "Always",
	Replicas:        5,
	MinReplicas:     5,
	MaxReplicas:     5,
	RayStartParams: map[string]string{
		"node-ip-address": "$MY_POD_IP",
	},
	ServiceAccount:  "account",
	ImagePullSecret: "foo",
	Environment: &api.EnvironmentVariables{
		Values: map[string]string{
			"foo": "bar",
		},
	},
	Annotations: map[string]string{
		"foo": "bar",
	},
	Labels: map[string]string{
		"foo": "bar",
	},
	SecurityContext: &api.SecurityContext{
		Capabilities: &api.Capabilities{
			Add: []string{"SYS_PTRACE"},
		},
	},
}

var rayCluster = api.Cluster{
	Name:      "test_cluster",
	Namespace: "foo",
	Annotations: map[string]string{
		"kubernetes.io/ingress.class": "nginx",
	},
	ClusterSpec: &api.ClusterSpec{
		HeadGroupSpec: &headGroup,
		WorkerGroupSpec: []*api.WorkerGroupSpec{
			&workerGroup,
		},
	},
}

var rayClusterAutoScaler = api.Cluster{
	Name:      "test_cluster",
	Namespace: "foo",
	Annotations: map[string]string{
		"kubernetes.io/ingress.class": "nginx",
	},
	ClusterSpec: &api.ClusterSpec{
		HeadGroupSpec: &headGroup,
		WorkerGroupSpec: []*api.WorkerGroupSpec{
			&workerGroup,
		},
		EnableInTreeAutoscaling: true,
		AutoscalerOptions: &api.AutoscalerOptions{
			UpscalingMode:      "Default",
			IdleTimeoutSeconds: 120,
			ImagePullPolicy:    "IfNotPresent",
		},
	},
}

var template = api.ComputeTemplate{
	Name:      "",
	Namespace: "",
	Cpu:       2,
	Memory:    8,
	Tolerations: []*api.PodToleration{
		{
			Key:      "blah1",
			Operator: "Exists",
			Effect:   "NoExecute",
		},
	},
}

var templateWorker = api.ComputeTemplate{
	Name:              "",
	Namespace:         "",
	Cpu:               2,
	Memory:            8,
	Gpu:               4,
	ExtendedResources: map[string]uint32{"vpc.amazonaws.com/efa": 32},
	Tolerations: []*api.PodToleration{
		{
			Key:      "blah1",
			Operator: "Exists",
			Effect:   "NoExecute",
		},
	},
	MemoryUnit: "Gi",
}

var expectedToleration = corev1.Toleration{
	Key:      "blah1",
	Operator: "Exists",
	Effect:   "NoExecute",
}

var expectedLabels = map[string]string{
	"foo": "bar",
}

var expectedHeadNodeEnv = []corev1.EnvVar{
	{
		Name: "MY_POD_IP",
		ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{
				FieldPath: "status.podIP",
			},
		},
	},
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
}

var expectedSecurityContext = corev1.SecurityContext{
	Capabilities: &corev1.Capabilities{
		Add: []corev1.Capability{
			"SYS_PTRACE",
		},
	},
}

func TestBuildVolumes(t *testing.T) {
	targetVolume := corev1.Volume{
		Name: testVolume.Name,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: testVolume.Source,
				Type: newHostPathType(string(corev1.HostPathDirectory)),
			},
		},
	}
	targetFileVolume := corev1.Volume{
		Name: testFileVolume.Name,
		VolumeSource: corev1.VolumeSource{
			HostPath: &corev1.HostPathVolumeSource{
				Path: testFileVolume.Source,
				Type: newHostPathType(string(corev1.HostPathFile)),
			},
		},
	}

	targetPVCVolume := corev1.Volume{
		Name: testPVCVolume.Name,
		VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: testPVCVolume.Source,
				ReadOnly:  testPVCVolume.ReadOnly,
			},
		},
	}

	targetEphemeralVolume := corev1.Volume{
		Name: testEphemeralVolume.Name,
		VolumeSource: corev1.VolumeSource{
			Ephemeral: &corev1.EphemeralVolumeSource{
				VolumeClaimTemplate: &corev1.PersistentVolumeClaimTemplate{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{
							"app.kubernetes.io/managed-by": "kuberay-apiserver",
						},
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(testEphemeralVolume.Storage),
							},
						},
					},
				},
			},
		},
	}

	targetConfigMapVolume := corev1.Volume{
		Name: "configMap",
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: "my-config-map",
				},
				Items: []corev1.KeyToPath{
					{
						Key:  "key1",
						Path: "path1",
					},
					{
						Key:  "key2",
						Path: "path2",
					},
				},
			},
		},
	}

	targetSecretVolume := corev1.Volume{
		Name: "secret",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName: "my-secret",
			},
		},
	}

	targetEmptyDirVolume := corev1.Volume{
		Name: "emptyDir",
		VolumeSource: corev1.VolumeSource{
			EmptyDir: &corev1.EmptyDirVolumeSource{
				SizeLimit: &sizelimit,
			},
		},
	}

	tests := []struct {
		name      string
		apiVolume []*api.Volume
		expect    []corev1.Volume
	}{
		{
			"normal test",
			[]*api.Volume{
				testVolume, testFileVolume,
			},
			[]corev1.Volume{targetVolume, targetFileVolume},
		},
		{
			"pvc test",
			[]*api.Volume{testPVCVolume},
			[]corev1.Volume{targetPVCVolume},
		},
		{
			"ephemeral test",
			[]*api.Volume{testEphemeralVolume},
			[]corev1.Volume{targetEphemeralVolume},
		},
		{
			"configmap test",
			[]*api.Volume{testConfigMapVolume},
			[]corev1.Volume{targetConfigMapVolume},
		},
		{
			"secret test",
			[]*api.Volume{testSecretVolume},
			[]corev1.Volume{targetSecretVolume},
		},
		{
			"empty dir test",
			[]*api.Volume{testEmptyDirVolume},
			[]corev1.Volume{targetEmptyDirVolume},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildVols(tt.apiVolume)
			require.NoError(t, err)
			if tt.name == "configmap test" {
				// Sort items for comparison
				sort.SliceStable(got[0].ConfigMap.Items, func(i, j int) bool {
					return got[0].ConfigMap.Items[i].Key < got[0].ConfigMap.Items[j].Key
				})
				sort.SliceStable(tt.expect[0].ConfigMap.Items, func(i, j int) bool {
					return tt.expect[0].ConfigMap.Items[i].Key < tt.expect[0].ConfigMap.Items[j].Key
				})
			}
			if !reflect.DeepEqual(got, tt.expect) {
				t.Errorf("failed for %s ..., got %v, expected %v", tt.name, got, tt.expect)
			}
		})
	}
}

func TestBuildVolumeMounts(t *testing.T) {
	hostToContainer := corev1.MountPropagationHostToContainer
	targetVolumeMount := corev1.VolumeMount{
		Name:      testVolume.Name,
		ReadOnly:  testVolume.ReadOnly,
		MountPath: testVolume.MountPath,
	}
	targetFileVolumeMount := corev1.VolumeMount{
		Name:             testFileVolume.Name,
		ReadOnly:         testFileVolume.ReadOnly,
		MountPath:        testFileVolume.MountPath,
		MountPropagation: &hostToContainer,
	}
	targetPVCVolumeMount := corev1.VolumeMount{
		Name:      testPVCVolume.Name,
		ReadOnly:  testPVCVolume.ReadOnly,
		MountPath: testPVCVolume.MountPath,
	}
	tests := []struct {
		name      string
		apiVolume []*api.Volume
		expect    []corev1.VolumeMount
	}{
		{
			"normal test",
			[]*api.Volume{
				testVolume,
				testFileVolume,
			},
			[]corev1.VolumeMount{
				targetVolumeMount,
				targetFileVolumeMount,
			},
		},
		{
			"pvc test",
			[]*api.Volume{testPVCVolume},
			[]corev1.VolumeMount{targetPVCVolumeMount},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildVolumeMounts(tt.apiVolume)
			if !reflect.DeepEqual(got, tt.expect) {
				t.Errorf("failed for %s ..., got %v, expected %v", tt.name, got, tt.expect)
			}
		})
	}
}

func TestBuildHeadPodTemplate(t *testing.T) {
	podSpec, err := buildHeadPodTemplate("2.4", &api.EnvironmentVariables{}, &headGroup, &template, false)
	require.NoError(t, err)

	if podSpec.Spec.ServiceAccountName != "account" {
		t.Errorf("failed to propagate service account")
	}
	if podSpec.Spec.ImagePullSecrets[0].Name != "foo" {
		t.Errorf("failed to propagate image pull secret")
	}
	if (string)(podSpec.Spec.Containers[0].ImagePullPolicy) != "Always" {
		t.Errorf("failed to propagate image pull policy")
	}
	if len(podSpec.Spec.Containers[0].Env) != 6 {
		t.Errorf("failed to propagate environment")
	}
	if len(podSpec.Spec.Containers[0].Ports) != 4 {
		t.Errorf("failed build ports")
	}
	// Sort values for comparison
	sort.SliceStable(podSpec.Spec.Containers[0].Env, func(i, j int) bool {
		return podSpec.Spec.Containers[0].Env[i].Name < podSpec.Spec.Containers[0].Env[j].Name
	})
	sort.SliceStable(expectedHeadNodeEnv, func(i, j int) bool {
		return expectedHeadNodeEnv[i].Name < expectedHeadNodeEnv[j].Name
	})

	if !reflect.DeepEqual(podSpec.Spec.Containers[0].Env, expectedHeadNodeEnv) {
		t.Errorf("failed to convert environment, got %v, expected %v", podSpec.Spec.Containers[0].Env, expectedHeadNodeEnv)
	}

	if len(podSpec.Spec.Tolerations) != 1 {
		t.Errorf("failed to propagate tolerations, expected 1, got %d", len(podSpec.Spec.Tolerations))
	}
	if !reflect.DeepEqual(podSpec.Spec.Tolerations[0], expectedToleration) {
		t.Errorf("failed to propagate annotations, got %v, expected %v", tolerationToString(&podSpec.Spec.Tolerations[0]),
			tolerationToString(&expectedToleration))
	}
	if val, exists := podSpec.Annotations["foo"]; !exists || val != "bar" {
		t.Errorf("failed to convert annotations")
	}
	if !reflect.DeepEqual(podSpec.Labels, expectedLabels) {
		t.Errorf("failed to convert labels, got %v, expected %v", podSpec.Labels, expectedLabels)
	}

	if !reflect.DeepEqual(podSpec.Spec.Containers[0].SecurityContext, &expectedSecurityContext) {
		t.Errorf("failed to convert security context, got %v, expected %v", podSpec.Spec.SecurityContext, &expectedSecurityContext)
	}

	podSpec, err = buildHeadPodTemplate("2.4", &api.EnvironmentVariables{}, &headGroup, &template, true)
	require.NoError(t, err)
	if len(podSpec.Spec.Containers[0].Ports) != 6 {
		t.Errorf("failed build ports")
	}
}

func TestNewComputeTemplate(t *testing.T) {
	configMap, err := NewComputeTemplate(&templateWorker)
	if err != nil {
		t.Errorf("failed to build compute template: %v", err)
	}

	assert.Empty(t, configMap.Data["name"])
	assert.Empty(t, configMap.Data["namespace"])
	assert.Equal(t, "2", configMap.Data["cpu"])
	assert.Equal(t, "8", configMap.Data["memory"])
	assert.Equal(t, "4", configMap.Data["gpu"])

	var ext map[string]uint32
	err = json.Unmarshal([]byte(configMap.Data["extended_resources"]), &ext)
	if err != nil {
		t.Errorf("failed to unmarshall ExtendedResources: %v", err)
	}
	assert.Equal(t, uint32(32), ext["vpc.amazonaws.com/efa"])

	var tolerations []*api.PodToleration
	err = json.Unmarshal([]byte(configMap.Data["tolerations"]), &tolerations)
	if err != nil {
		t.Errorf("failed to unmarshall tolerations: %v", err)
	}
	assert.Equal(t, expectedToleration.Key, tolerations[0].Key)
	assert.Equal(t, string(expectedToleration.Operator), tolerations[0].Operator)
	assert.Equal(t, string(expectedToleration.Effect), tolerations[0].Effect)
}

func TestGetNodeHostIP(t *testing.T) {
	internalIP := "10.0.0.1"
	externalIP := "12.34.56.78"
	invalidIP := "invalid-ip-address"

	tests := []struct {
		name        string
		expectIP    string
		expectError string
		addresses   []corev1.NodeAddress
	}{
		{
			name: "InternalOnly",
			addresses: []corev1.NodeAddress{
				{Type: corev1.NodeInternalIP, Address: internalIP},
			},
			expectIP: internalIP,
		},
		{
			name: "ExternalOnly",
			addresses: []corev1.NodeAddress{
				{Type: corev1.NodeExternalIP, Address: externalIP},
			},
			expectIP: externalIP,
		},
		{
			name: "InternalAndExternal",
			addresses: []corev1.NodeAddress{
				{Type: corev1.NodeExternalIP, Address: externalIP},
				{Type: corev1.NodeInternalIP, Address: internalIP},
			},
			expectIP: internalIP,
		},
		{
			name: "NoValidIP",
			addresses: []corev1.NodeAddress{
				{Type: corev1.NodeHostName, Address: invalidIP},
			},
			expectError: "host IP unknown; known addresses: [{Hostname invalid-ip-address}]",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node := &corev1.Node{
				Status: corev1.NodeStatus{
					Addresses: tc.addresses,
				},
			}
			ip, err := GetNodeHostIP(node)

			if tc.expectError != "" {
				assert.EqualError(t, err, tc.expectError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectIP, ip.String())
			}
		})
	}
}

func TestConvertAutoscalerOptions(t *testing.T) {
	options, err := buildAutoscalerOptions(&testAutoscalerOptions)
	require.NoError(t, err)
	assert.Equal(t, int32(25), *options.IdleTimeoutSeconds)
	assert.Equal(t, (string)(*options.UpscalingMode), "Default")
	assert.Equal(t, (string)(*options.ImagePullPolicy), "Always")
	assert.Len(t, options.Env, 1)
	assert.Len(t, options.EnvFrom, 2)
	assert.Len(t, options.VolumeMounts, 2)
	assert.Equal(t, "300m", options.Resources.Requests.Cpu().String())
	assert.Equal(t, "512Mi", options.Resources.Requests.Memory().String())
}

func TestBuildRayCluster(t *testing.T) {
	cluster, err := NewRayCluster(&rayCluster, map[string]*api.ComputeTemplate{"foo": &template})
	require.NoError(t, err)
	if len(cluster.ObjectMeta.Annotations) != 1 {
		t.Errorf("failed to propagate annotations")
	}
	if !(*cluster.Spec.HeadGroupSpec.EnableIngress) {
		t.Errorf("failed to propagate create Ingress")
	}
	assert.Equal(t, (*bool)(nil), cluster.Spec.EnableInTreeAutoscaling)
	cluster, err = NewRayCluster(&rayClusterAutoScaler, map[string]*api.ComputeTemplate{"foo": &template})
	require.NoError(t, err)
	assert.True(t, *cluster.Spec.EnableInTreeAutoscaling)
	assert.NotNil(t, cluster.Spec.AutoscalerOptions)
}

func TestBuilWorkerPodTemplate(t *testing.T) {
	podSpec, err := buildWorkerPodTemplate("2.4", &api.EnvironmentVariables{}, &workerGroup, &templateWorker)
	require.NoError(t, err)

	assert.Equal(t, "account", podSpec.Spec.ServiceAccountName, "failed to propagate service account")
	assert.Equal(t, "foo", podSpec.Spec.ImagePullSecrets[0].Name, "failed to propagate image pull secret")
	assert.Equal(t, corev1.PullAlways, podSpec.Spec.Containers[0].ImagePullPolicy, "failed to propagate image pull policy")
	assert.True(t, containsEnv(podSpec.Spec.Containers[0].Env, "foo", "bar"), "failed to propagate environment")
	assert.Len(t, podSpec.Spec.Tolerations, 1, "failed to propagate tolerations")
	assert.Equal(t, expectedToleration, podSpec.Spec.Tolerations[0], "failed to propagate tolerations")
	assert.Equal(t, "bar", podSpec.Annotations["foo"], "failed to convert annotations")
	assert.Equal(t, expectedLabels, podSpec.Labels, "failed to convert labels")
	assert.True(t, containsEnvValueFrom(podSpec.Spec.Containers[0].Env, "CPU_REQUEST", &corev1.EnvVarSource{ResourceFieldRef: &corev1.ResourceFieldSelector{ContainerName: "ray-worker", Resource: "requests.cpu"}}), "failed to propagate environment variable: CPU_REQUEST")
	assert.True(t, containsEnvValueFrom(podSpec.Spec.Containers[0].Env, "CPU_LIMITS", &corev1.EnvVarSource{ResourceFieldRef: &corev1.ResourceFieldSelector{ContainerName: "ray-worker", Resource: "limits.cpu"}}), "failed to propagate environment variable: CPU_LIMITS")
	assert.True(t, containsEnvValueFrom(podSpec.Spec.Containers[0].Env, "MEMORY_REQUESTS", &corev1.EnvVarSource{ResourceFieldRef: &corev1.ResourceFieldSelector{ContainerName: "ray-worker", Resource: "requests.memory"}}), "failed to propagate environment variable: MEMORY_REQUESTS")
	assert.True(t, containsEnvValueFrom(podSpec.Spec.Containers[0].Env, "MEMORY_LIMITS", &corev1.EnvVarSource{ResourceFieldRef: &corev1.ResourceFieldSelector{ContainerName: "ray-worker", Resource: "limits.memory"}}), "failed to propagate environment variable: MEMORY_LIMITS")
	assert.True(t, containsEnvValueFrom(podSpec.Spec.Containers[0].Env, "MY_POD_NAME", &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}), "failed to propagate environment variable: MY_POD_NAME")
	assert.True(t, containsEnvValueFrom(podSpec.Spec.Containers[0].Env, "MY_POD_IP", &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}), "failed to propagate environment variable: MY_POD_IP")
	assert.Equal(t, &expectedSecurityContext, podSpec.Spec.Containers[0].SecurityContext, "failed to convert security context")

	// Check Resources
	container := podSpec.Spec.Containers[0]
	resources := container.Resources

	assert.Equal(t, resource.MustParse("2"), resources.Limits[corev1.ResourceCPU], "CPU limit doesn't match")
	assert.Equal(t, resource.MustParse("2"), resources.Requests[corev1.ResourceCPU], "CPU request doesn't match")

	assert.Equal(t, resource.MustParse("8Gi"), resources.Limits[corev1.ResourceMemory], "Memory limit doesn't match")
	assert.Equal(t, resource.MustParse("8Gi"), resources.Requests[corev1.ResourceMemory], "Memory request doesn't match")

	assert.Equal(t, resource.MustParse("4"), resources.Limits["nvidia.com/gpu"], "GPU limit doesn't match")
	assert.Equal(t, resource.MustParse("4"), resources.Requests["nvidia.com/gpu"], "GPU request doesn't match")

	assert.Equal(t, resource.MustParse("32"), resources.Limits["vpc.amazonaws.com/efa"], "EFA limit doesn't match")
	assert.Equal(t, resource.MustParse("32"), resources.Requests["vpc.amazonaws.com/efa"], "EFA request doesn't match")
}

func containsEnv(envs []corev1.EnvVar, key string, val string) bool {
	for _, env := range envs {
		if env.Name == key && env.Value == val {
			return true
		}
	}
	return false
}

func containsEnvValueFrom(envs []corev1.EnvVar, key string, valFrom *corev1.EnvVarSource) bool {
	for _, env := range envs {
		if env.Name == key && reflect.DeepEqual(env.ValueFrom, valFrom) {
			return true
		}
	}
	return false
}

func tolerationToString(toleration *corev1.Toleration) string {
	return "Key: " + toleration.Key + " Operator: " + string(toleration.Operator) + " Effect: " + string(toleration.Effect)
}
