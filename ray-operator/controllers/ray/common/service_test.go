package common

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

var (
	headServiceAnnotationKey1   = "HeadServiceAnnotationKey1"
	headServiceAnnotationValue1 = "HeadServiceAnnotationValue1"
	headServiceAnnotationKey2   = "HeadServiceAnnotationKey2"
	headServiceAnnotationValue2 = "HeadServiceAnnotationValue2"
	serviceInstance             = &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "rayservice-sample",
			Namespace: "default",
		},
		Spec: rayv1.RayServiceSpec{
			RayClusterSpec: rayv1.RayClusterSpec{
				HeadGroupSpec: rayv1.HeadGroupSpec{
					ServiceType: corev1.ServiceTypeClusterIP,
				},
			},
		},
	}
	instanceWithWrongSvc = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadServiceAnnotations: map[string]string{
				headServiceAnnotationKey1: headServiceAnnotationValue1,
				headServiceAnnotationKey2: headServiceAnnotationValue2,
			},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				RayStartParams: map[string]string{
					"port":                "6379",
					"object-manager-port": "12345",
					"node-manager-port":   "12346",
					"object-store-memory": "100000000",
					"num-cpus":            "1",
				},
				Template: corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: "default",
						Labels: map[string]string{
							"groupName": utils.RayNodeHeadGroupLabelValue,
						},
					},
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "ray-head",
								Image: "rayproject/autoscaler",
								Ports: []corev1.ContainerPort{
									{
										ContainerPort: 6379,
										Name:          utils.GcsServerPortName,
									},
									{
										ContainerPort: 8265,
										Name:          utils.DashboardPortName,
									},
									{
										ContainerPort: 8000,
										Name:          utils.ServingPortName,
									},
								},
								Command: []string{"python"},
								Args:    []string{"/opt/code.py"},
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
							},
						},
					},
				},
			},
		},
	}
	instanceForSvc = &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample-svc",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadServiceAnnotations: map[string]string{
				headServiceAnnotationKey1: headServiceAnnotationValue1,
				headServiceAnnotationKey2: headServiceAnnotationValue2,
			},
			HeadGroupSpec: rayv1.HeadGroupSpec{
				ServiceType: corev1.ServiceTypeClusterIP,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "ray-head",
								Ports: []corev1.ContainerPort{
									{ContainerPort: 8000, Name: "serve"},
								},
							},
						},
					},
				},
			},
		},
	}
)

func TestBuildServiceForHeadPod(t *testing.T) {
	svc, err := BuildServiceForHeadPod(context.Background(), *instanceWithWrongSvc, nil, nil)
	require.NoError(t, err)

	assert.Equal(t, instanceWithWrongSvc.Name, svc.Spec.Selector[utils.RayClusterLabelKey])
	assert.Equal(t, string(rayv1.HeadNode), svc.Spec.Selector[utils.RayNodeTypeLabelKey])
	assert.Equal(t, utils.ApplicationName, svc.Spec.Selector[utils.KubernetesApplicationNameLabelKey])

	for _, port := range svc.Spec.Ports {
		assert.Equal(t, utils.DefaultServiceAppProtocol, *port.AppProtocol)
	}

	// BuildServiceForHeadPod should generate a headless service for a Head Pod by default.
	assert.Equal(t, corev1.ClusterIPNone, svc.Spec.ClusterIP)
}

// Test that default ports are applied when none are specified. The metrics
// port is always added if not explicitly set.
func TestBuildServiceForHeadPodDefaultPorts(t *testing.T) {
	type testCase struct {
		name         string
		expectResult map[string]int32
		ports        []corev1.ContainerPort
	}

	testCases := []testCase{
		{
			name:         "No ports are specified by the user.",
			ports:        []corev1.ContainerPort{},
			expectResult: getDefaultPorts(),
		},
		{
			name: "Only a random port is specified by the user.",
			ports: []corev1.ContainerPort{
				{
					Name:          "random",
					ContainerPort: 1234,
				},
			},
			expectResult: map[string]int32{
				"random": 1234,
				// metrics port will always be there
				utils.MetricsPortName: utils.DefaultMetricsPort,
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cluster := instanceWithWrongSvc.DeepCopy()
			cluster.Spec.HeadGroupSpec.Template.Spec.Containers[0].Ports = testCase.ports
			svc, err := BuildServiceForHeadPod(context.Background(), *cluster, nil, nil)

			require.NoError(t, err)
			ports := svc.Spec.Ports

			svcPorts := make(map[string]int32)
			for _, port := range ports {
				svcPorts[port.Name] = port.Port
			}

			assert.Equal(t, testCase.expectResult, svcPorts)
		})
	}
}

func TestBuildClusterIPServiceForHeadPod(t *testing.T) {
	os.Setenv(utils.ENABLE_RAY_HEAD_CLUSTER_IP_SERVICE, "true")
	defer os.Unsetenv(utils.ENABLE_RAY_HEAD_CLUSTER_IP_SERVICE)
	svc, err := BuildServiceForHeadPod(context.Background(), *instanceWithWrongSvc, nil, nil)
	require.NoError(t, err)
	// BuildServiceForHeadPod should not generate a headless service for a Head Pod if ENABLE_RAY_HEAD_CLUSTER_IP_SERVICE is set.
	assert.NotEqual(t, corev1.ClusterIPNone, svc.Spec.ClusterIP)
}

func TestBuildServiceForHeadPodWithAppNameLabel(t *testing.T) {
	labels := make(map[string]string)
	labels[utils.KubernetesApplicationNameLabelKey] = "testname"

	svc, err := BuildServiceForHeadPod(context.Background(), *instanceWithWrongSvc, labels, nil)
	require.NoError(t, err)

	assert.Equal(t, "testname", svc.Spec.Selector[utils.KubernetesApplicationNameLabelKey])

	// We have 5 default labels in `BuildServiceForHeadPod`, and `KubernetesApplicationNameLabelKey`
	// is one of the default labels. Hence, `expectedLength` should also be 5.
	assert.Len(t, svc.Spec.Selector, 5)
}

func TestBuildServiceForHeadPodWithAnnotations(t *testing.T) {
	annotations := make(map[string]string)
	annotations["key1"] = "testvalue1"
	annotations["key2"] = "testvalue2"
	svc, err := BuildServiceForHeadPod(context.Background(), *instanceWithWrongSvc, nil, annotations)
	require.NoError(t, err)

	assert.Equal(t, annotations, svc.ObjectMeta.Annotations)
}

func TestGetPortsFromCluster(t *testing.T) {
	svcPorts := getPortsFromCluster(*instanceWithWrongSvc)

	// getPortsFromCluster creates service ports based on the container ports.
	// It will assign a generated service port name if the container port name
	// is not defined. To compare created service ports with container ports,
	// all generated service port names need to be reverted to empty strings.
	svcNames := map[int32]string{}
	for name, port := range svcPorts {
		if name == (fmt.Sprint(port) + "-port") {
			name = ""
		}
		svcNames[port] = name
	}

	cPorts := instanceWithWrongSvc.Spec.HeadGroupSpec.Template.Spec.Containers[utils.RayContainerIndex].Ports

	for _, cPort := range cPorts {
		assert.Equal(t, cPort.Name, svcNames[cPort.ContainerPort])
	}
}

func TestGetServicePortsWithMetricsPort(t *testing.T) {
	testCases := []struct {
		name         string
		ports        []corev1.ContainerPort
		expectResult int32
	}{
		{
			name:         "No ports are specified by the user.",
			ports:        []corev1.ContainerPort{},
			expectResult: int32(utils.DefaultMetricsPort),
		},
		{
			name: "Only a random port is specified by the user.",
			ports: []corev1.ContainerPort{
				{
					Name:          "random",
					ContainerPort: 1234,
				},
			},
			expectResult: int32(utils.DefaultMetricsPort),
		},
		{
			name: "A custom port is specified by the user.",
			ports: []corev1.ContainerPort{
				{
					Name:          utils.MetricsPortName,
					ContainerPort: int32(utils.DefaultMetricsPort) + 1,
				},
			},
			expectResult: int32(utils.DefaultMetricsPort) + 1,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			cluster := instanceWithWrongSvc.DeepCopy()
			cluster.Spec.HeadGroupSpec.Template.Spec.Containers[0].Ports = testCase.ports
			ports := getServicePorts(*cluster)
			assert.Equal(t, testCase.expectResult, ports[utils.MetricsPortName])
		})
	}
}

func TestUserSpecifiedHeadService(t *testing.T) {
	// Use any RayCluster instance as a base for the test.
	testRayClusterWithHeadService := instanceWithWrongSvc.DeepCopy()

	// Set user-specified head service with user-specified labels, annotations, and ports.
	userName := "user-custom-name"
	userNamespace := "user-custom-namespace"
	userLabels := map[string]string{"userLabelKey": "userLabelValue", utils.RayClusterLabelKey: "userClusterName"} // Override default cluster name
	userAnnotations := map[string]string{"userAnnotationKey": "userAnnotationValue", headServiceAnnotationKey1: "user_override"}
	userPort := corev1.ServicePort{Name: "userPort", Port: 12345}
	userPortOverride := corev1.ServicePort{Name: utils.ClientPortName, Port: 98765} // Override default client port (10001)
	userPorts := []corev1.ServicePort{userPort, userPortOverride}
	userSelector := map[string]string{"userSelectorKey": "userSelectorValue", utils.RayClusterLabelKey: "userSelectorClusterName"}
	// Specify a "LoadBalancer" type, which differs from the default "ClusterIP" type.
	userType := corev1.ServiceTypeLoadBalancer
	// Specify an empty ClusterIP, which differs from the default "None" used by the BuildServeServiceForRayService.
	userClusterIP := ""
	testRayClusterWithHeadService.Spec.HeadGroupSpec.HeadService = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        userName,
			Namespace:   userNamespace,
			Labels:      userLabels,
			Annotations: userAnnotations,
		},
		Spec: corev1.ServiceSpec{
			Ports:     userPorts,
			Selector:  userSelector,
			Type:      userType,
			ClusterIP: userClusterIP,
		},
	}
	// These labels originate from HeadGroupSpec.Template.ObjectMeta.Labels
	userTemplateClusterName := "userTemplateClusterName"
	templateLabels := map[string]string{utils.RayClusterLabelKey: userTemplateClusterName}
	headService, err := BuildServiceForHeadPod(context.Background(), *testRayClusterWithHeadService, templateLabels, testRayClusterWithHeadService.Spec.HeadServiceAnnotations)
	if err != nil {
		t.Errorf("failed to build head service: %v", err)
	}

	// BuildServiceForHeadPod should respect the ClusterIP specified by users.
	assert.Equal(t, userClusterIP, headService.Spec.ClusterIP)

	// The selector field should only use the keys from the five default labels.  The values should be updated with the values from the template labels.
	// The user-provided HeadService labels should be ignored for the purposes of the selector field. The user-provided Selector field should be ignored.
	defaultLabels := HeadServiceLabels(*testRayClusterWithHeadService)
	// Make sure this test isn't spuriously passing. Check that RayClusterLabelKey is in the default labels.
	if _, ok := defaultLabels[utils.RayClusterLabelKey]; !ok {
		t.Errorf("utils.RayClusterLabelKey=%s should be in the default labels", utils.RayClusterLabelKey)
	}
	for k, v := range headService.Spec.Selector {
		// If k is not in the default labels, then the selector field should not contain it.
		if _, ok := defaultLabels[k]; !ok {
			t.Errorf("Selector field should not contain key=%s", k)
		}
		// If k is in the template labels, then the selector field should contain it with the value from the template labels.
		// Otherwise, it should contain the value from the default labels.
		if _, ok := templateLabels[k]; ok {
			if v != templateLabels[k] {
				t.Errorf("Selector field should contain key=%s with value=%s, actual value=%s", k, templateLabels[k], v)
			}
		} else {
			if v != defaultLabels[k] {
				t.Errorf("Selector field should contain key=%s with value=%s, actual value=%s", k, defaultLabels[k], v)
			}
		}
	}
	// The selector field should have every key from the default labels.
	for k := range defaultLabels {
		if _, ok := headService.Spec.Selector[k]; !ok {
			t.Errorf("Selector field should contain key=%s", k)
		}
	}

	// Print default labels for debugging
	for k, v := range defaultLabels {
		fmt.Printf("default label: key=%s, value=%s\n", k, v)
	}

	// Test merged labels. The final labels (headService.ObjectMeta.Labels) should consist of:
	// 1. The final selector (headService.Spec.Selector), updated with
	// 2. The user-specified labels from the HeadService (userLabels).
	// In the case of overlap, the selector labels have priority over userLabels.
	for k, v := range headService.ObjectMeta.Labels {
		// If k is in the user-specified labels, then the final labels should contain it with the value from the final selector.
		// Otherwise, it should contain the value from userLabels from the HeadService.
		if _, ok := headService.Spec.Selector[k]; ok {
			if v != headService.Spec.Selector[k] {
				t.Errorf("Final labels should contain key=%s with value=%s, actual value=%s", k, headService.Spec.Selector[k], v)
			}
		} else if _, ok := userLabels[k]; ok {
			if v != userLabels[k] {
				t.Errorf("Final labels should contain key=%s with value=%s, actual value=%s", k, userLabels[k], v)
			}
		} else {
			t.Errorf("Final labels contains key=%s but it should not", k)
		}
	}
	// Check that every key from the final selector (headService.Spec.Selector) and userLabels is in the final labels.
	for k := range headService.Spec.Selector {
		if _, ok := headService.ObjectMeta.Labels[k]; !ok {
			t.Errorf("Final labels should contain key=%s", k)
		}
	}

	// Test merged annotations. In the case of overlap (HeadServiceAnnotationKey1) the user annotation should be ignored.
	for k, v := range userAnnotations {
		if headService.ObjectMeta.Annotations[k] != v && k != headServiceAnnotationKey1 {
			t.Errorf("User annotation not found or incorrect value: key=%s, expected value=%s, actual value=%s", k, v, headService.ObjectMeta.Annotations[k])
		}
	}
	if headService.ObjectMeta.Annotations[headServiceAnnotationKey1] != headServiceAnnotationValue1 {
		t.Errorf("User annotation not found or incorrect value: key=%s, expected value=%s, actual value=%s", headServiceAnnotationKey1, headServiceAnnotationValue1, headService.ObjectMeta.Annotations[headServiceAnnotationKey1])
	}
	// HeadServiceAnnotationKey2 should be present with value HeadServiceAnnotationValue2 since it was only specified in HeadServiceAnnotations.
	if headService.ObjectMeta.Annotations[headServiceAnnotationKey2] != headServiceAnnotationValue2 {
		t.Errorf("User annotation not found or incorrect value: key=%s, expected value=%s, actual value=%s", headServiceAnnotationKey2, headServiceAnnotationValue2, headService.ObjectMeta.Annotations[headServiceAnnotationKey2])
	}

	// Test merged ports. In the case of overlap (ClientPortName) the user port should be ignored.
	// DEBUG: Print out the entire head service to help with debugging.
	headServiceJSON, err := json.MarshalIndent(headService, "", "  ")
	if err != nil {
		t.Errorf("failed to marshal head service: %v", err)
	}
	t.Logf("head service: %s", string(headServiceJSON))

	// Test merged ports
	for _, p := range userPorts {
		found := false
		for _, hp := range headService.Spec.Ports {
			if p.Name == hp.Name && p.Port == hp.Port {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("User port not found: %v", p)
		}
	}

	validateServiceTypeForUserSpecifiedService(headService, userType, t)
	validateLabelsForUserSpecifiedService(headService, userLabels, t)
	validateNameAndNamespaceForUserSpecifiedService(headService, testRayClusterWithHeadService.ObjectMeta.Namespace, userName, t)
}

func TestNilMapDoesntErrorInUserSpecifiedHeadService(t *testing.T) {
	// Use any RayCluster instance as a base for the test.
	testRayClusterWithHeadService := instanceWithWrongSvc.DeepCopy()

	// Set user-specified head service with many nil fields.
	testRayClusterWithHeadService.Spec.HeadGroupSpec.HeadService = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{},
	}

	_, err := BuildServiceForHeadPod(context.Background(), *testRayClusterWithHeadService, nil, nil)
	if err != nil {
		t.Errorf("failed to build head service: %v", err)
	}
}

func TestBuildServiceForHeadPodPortsOrder(t *testing.T) {
	ctx := context.Background()
	svc1, err1 := BuildServiceForHeadPod(ctx, *instanceWithWrongSvc, nil, nil)
	svc2, err2 := BuildServiceForHeadPod(ctx, *instanceWithWrongSvc, nil, nil)
	require.NoError(t, err1)
	require.NoError(t, err2)

	ports1 := svc1.Spec.Ports
	ports2 := svc2.Spec.Ports

	// length should be same
	assert.Len(t, ports2, len(ports1))
	for i := range ports1 {
		// name should be same
		assert.Equal(t, ports1[i].Name, ports2[i].Name)
	}
}

func TestBuildHeadlessServiceForRayCluster(t *testing.T) {
	svc := BuildHeadlessServiceForRayCluster(*instanceForSvc)

	assert.Equal(t, instanceForSvc.Name, svc.Spec.Selector[utils.RayClusterLabelKey])
	assert.Equal(t, string(rayv1.WorkerNode), svc.Spec.Selector[utils.RayNodeTypeLabelKey])
	assert.Equal(t, instanceForSvc.Name, svc.Labels[utils.RayClusterHeadlessServiceLabelKey])
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)
	assert.Equal(t, corev1.ClusterIPNone, svc.Spec.ClusterIP)
	assert.True(t, svc.Spec.PublishNotReadyAddresses)

	expectedName := fmt.Sprintf("%s-%s", instanceForSvc.Name, utils.HeadlessServiceSuffix)
	validateNameAndNamespaceForUserSpecifiedService(svc, serviceInstance.ObjectMeta.Namespace, expectedName, t)
}

func TestBuildServeServiceForRayService(t *testing.T) {
	svc, err := BuildServeServiceForRayService(context.Background(), *serviceInstance, *instanceWithWrongSvc)
	require.NoError(t, err)

	assert.Equal(t, instanceWithWrongSvc.Name, svc.Spec.Selector[utils.RayClusterLabelKey])
	assert.Equal(t, serviceInstance.Name, svc.Labels[utils.RayOriginatedFromCRNameLabelKey])
	assert.Equal(t, utils.RayOriginatedFromCRDLabelValue(utils.RayServiceCRD), svc.Labels[utils.RayOriginatedFromCRDLabelKey])
	assert.Equal(t, corev1.ServiceTypeClusterIP, svc.Spec.Type)

	expectedName := fmt.Sprintf("%s-%s-%s", serviceInstance.Name, "serve", "svc")
	validateNameAndNamespaceForUserSpecifiedService(svc, serviceInstance.ObjectMeta.Namespace, expectedName, t)
}

func TestBuildServeServiceForRayCluster(t *testing.T) {
	svc, err := BuildServeServiceForRayCluster(context.Background(), *instanceForSvc)
	require.NoError(t, err)

	assert.Equal(t, instanceForSvc.Name, svc.Spec.Selector[utils.RayClusterLabelKey])
	assert.Equal(t, instanceForSvc.Name, svc.Labels[utils.RayOriginatedFromCRNameLabelKey])
	assert.Equal(t, utils.RayOriginatedFromCRDLabelValue(utils.RayClusterCRD), svc.Labels[utils.RayOriginatedFromCRDLabelKey])
	assert.Equal(t, instanceForSvc.Spec.HeadGroupSpec.ServiceType, svc.Spec.Type)

	expectedName := fmt.Sprintf("%s-%s-%s", instanceForSvc.Name, "serve", "svc")
	validateNameAndNamespaceForUserSpecifiedService(svc, serviceInstance.ObjectMeta.Namespace, expectedName, t)
}

func TestBuildServeServiceForRayService_WithoutServePort(t *testing.T) {
	// Create a RayCluster without a port with the name "serve" in the Ray head container.
	cluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-sample",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "ray-head",
								Ports: []corev1.ContainerPort{
									{ContainerPort: 6379, Name: utils.GcsServerPortName},
								},
							},
						},
					},
				},
			},
		},
	}
	svc, err := BuildServeServiceForRayService(context.Background(), *serviceInstance, cluster)
	require.Error(t, err)
	assert.Nil(t, svc)
}

func TestUserSpecifiedServeService(t *testing.T) {
	// Use any RayService instance as a base for the test.
	testRayServiceWithServeService := serviceInstance.DeepCopy()

	userName := "user-custom-name"
	userNamespace := "user-custom-namespace"
	userLabels := map[string]string{"userLabelKey": "userLabelValue", utils.RayClusterLabelKey: "userClusterName"} // Override default cluster name
	userAnnotations := map[string]string{"userAnnotationKey": "userAnnotationValue", "userAnnotationKey2": "userAnnotationValue2"}
	userPort := corev1.ServicePort{Name: "serve", Port: 12345}
	userPortOverride := corev1.ServicePort{Name: utils.ClientPortName, Port: 98765} // Override default client port (10001)
	userPorts := []corev1.ServicePort{userPort, userPortOverride}
	userSelector := map[string]string{"userSelectorKey": "userSelectorValue", utils.RayClusterLabelKey: "userSelectorClusterName"}
	// Specify a "LoadBalancer" type, which differs from the default "ClusterIP" type.
	userType := corev1.ServiceTypeLoadBalancer

	testRayServiceWithServeService.Spec.ServeService = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:        userName,
			Namespace:   userNamespace,
			Labels:      userLabels,
			Annotations: userAnnotations,
		},
		Spec: corev1.ServiceSpec{
			Ports:    userPorts,
			Selector: userSelector,
			Type:     userType,
		},
	}

	svc, err := BuildServeServiceForRayService(context.Background(), *testRayServiceWithServeService, *instanceWithWrongSvc)
	if err != nil {
		t.Errorf("failed to build serve service: %v", err)
	}

	// Check every annotation is in the service annotation
	for k := range userAnnotations {
		if _, ok := svc.ObjectMeta.Annotations[k]; !ok {
			t.Errorf("Final labels should contain key=%s", k)
		}
	}

	// Check that selectors only have default selectors
	if len(svc.Spec.Selector) != 2 {
		t.Errorf("Selectors should have just 2 keys %s and %s", utils.RayClusterLabelKey, utils.RayClusterServingServiceLabelKey)
	}
	if svc.Spec.Selector[utils.RayClusterLabelKey] != instanceWithWrongSvc.Name {
		t.Errorf("Serve Service selector key %s value didn't match expected value : expected value=%s, actual value=%s", utils.RayClusterLabelKey, instanceWithWrongSvc.Name, svc.Spec.Selector[utils.RayClusterLabelKey])
	}
	if svc.Spec.Selector[utils.RayClusterServingServiceLabelKey] != utils.EnableRayClusterServingServiceTrue {
		t.Errorf("Serve Service selector key %s value didn't match expected value : expected value=%s, actual value=%s", utils.RayClusterServingServiceLabelKey, utils.EnableRayClusterServingServiceTrue, svc.Spec.Selector[utils.RayClusterServingServiceLabelKey])
	}

	// When user specifies a port with the same name as an auto-detected port,
	// the user's definition takes precedence (preserving fields like AppProtocol).
	assert.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, utils.ServingPortName, svc.Spec.Ports[0].Name)
	assert.Equal(t, int32(12345), svc.Spec.Ports[0].Port)

	validateServiceTypeForUserSpecifiedService(svc, userType, t)
	validateLabelsForUserSpecifiedService(svc, userLabels, t)
	validateNameAndNamespaceForUserSpecifiedService(svc, testRayServiceWithServeService.ObjectMeta.Namespace, userName, t)
}

func TestBuildServeServiceWithGrpcPort(t *testing.T) {
	cluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-grpc",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				ServiceType: corev1.ServiceTypeClusterIP,
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "ray-head",
								Ports: []corev1.ContainerPort{
									{ContainerPort: 6379, Name: utils.GcsServerPortName},
									{ContainerPort: 8265, Name: utils.DashboardPortName},
									{ContainerPort: 8000, Name: utils.ServingPortName},
									{ContainerPort: 9000, Name: "serve-grpc"},
								},
							},
						},
					},
				},
			},
		},
	}

	svc, err := BuildServeServiceForRayCluster(context.Background(), cluster)
	require.NoError(t, err)

	// The serve service should include both "serve" (HTTP) and "serve-grpc" (gRPC) ports
	assert.Len(t, svc.Spec.Ports, 2)

	portsByName := map[string]int32{}
	for _, p := range svc.Spec.Ports {
		portsByName[p.Name] = p.Port
	}
	assert.Equal(t, int32(8000), portsByName[utils.ServingPortName])
	assert.Equal(t, int32(9000), portsByName["serve-grpc"])

	// Internal ports like gcs-server and dashboard should NOT be included
	_, hasGcs := portsByName[utils.GcsServerPortName]
	_, hasDashboard := portsByName[utils.DashboardPortName]
	assert.False(t, hasGcs, "gcs-server port should not be in serve service")
	assert.False(t, hasDashboard, "dashboard port should not be in serve service")
}

func TestBuildServeServiceForRayServiceWithGrpcPort(t *testing.T) {
	cluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-grpc",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "ray-head",
								Ports: []corev1.ContainerPort{
									{ContainerPort: 6379, Name: utils.GcsServerPortName},
									{ContainerPort: 8000, Name: utils.ServingPortName},
									{ContainerPort: 9000, Name: "serve-grpc"},
								},
							},
						},
					},
				},
			},
		},
	}

	svc, err := BuildServeServiceForRayService(context.Background(), *serviceInstance, cluster)
	require.NoError(t, err)

	assert.Len(t, svc.Spec.Ports, 2)

	portsByName := map[string]int32{}
	for _, p := range svc.Spec.Ports {
		portsByName[p.Name] = p.Port
	}
	assert.Equal(t, int32(8000), portsByName[utils.ServingPortName])
	assert.Equal(t, int32(9000), portsByName["serve-grpc"])
}

func TestUserSpecifiedServeServiceWithGrpcPort(t *testing.T) {
	testRayServiceWithServeService := serviceInstance.DeepCopy()

	testRayServiceWithServeService.Spec.ServeService = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-serve-svc",
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: "serve", Port: 8000},
				{Name: "serve-grpc", Port: 9000},
			},
			Type: corev1.ServiceTypeLoadBalancer,
		},
	}

	// instanceWithWrongSvc has serve:8000 in its container ports.
	// The custom ServeService also specifies serve-grpc:9000, but since
	// auto-detected ports (serve:8000) are non-empty, only those are used.
	svc, err := BuildServeServiceForRayService(context.Background(), *testRayServiceWithServeService, *instanceWithWrongSvc)
	require.NoError(t, err)

	// Only the auto-detected serve port from the container spec is included
	assert.Len(t, svc.Spec.Ports, 1)
	assert.Equal(t, utils.ServingPortName, svc.Spec.Ports[0].Name)
	assert.Equal(t, int32(8000), svc.Spec.Ports[0].Port)
}

func TestUserSpecifiedServeServiceAppProtocolPreserved(t *testing.T) {
	cluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-grpc",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "ray-head",
								Ports: []corev1.ContainerPort{
									{ContainerPort: 8000, Name: utils.ServingPortName},
									{ContainerPort: 9000, Name: "serve-grpc"},
								},
							},
						},
					},
				},
			},
		},
	}

	h2c := "kubernetes.io/h2c"
	testRayService := serviceInstance.DeepCopy()
	testRayService.Spec.ServeService = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-serve-svc",
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: utils.ServingPortName, Port: 8000},
				{Name: "serve-grpc", Port: 9000, AppProtocol: &h2c},
			},
		},
	}

	svc, err := BuildServeServiceForRayService(context.Background(), *testRayService, cluster)
	require.NoError(t, err)

	assert.Len(t, svc.Spec.Ports, 2)

	portsByName := map[string]corev1.ServicePort{}
	for _, p := range svc.Spec.Ports {
		portsByName[p.Name] = p
	}

	// HTTP port has no appProtocol
	assert.Nil(t, portsByName[utils.ServingPortName].AppProtocol)

	// gRPC port preserves user-specified appProtocol
	require.NotNil(t, portsByName["serve-grpc"].AppProtocol)
	assert.Equal(t, "kubernetes.io/h2c", *portsByName["serve-grpc"].AppProtocol)
}

func TestUserSpecifiedServeServiceFallbackPreservesAppProtocol(t *testing.T) {
	// Cluster with NO serve ports in container spec
	cluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "raycluster-no-serve",
			Namespace: "default",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name: "ray-head",
								Ports: []corev1.ContainerPort{
									{ContainerPort: 6379, Name: utils.GcsServerPortName},
								},
							},
						},
					},
				},
			},
		},
	}

	h2c := "kubernetes.io/h2c"
	testRayService := serviceInstance.DeepCopy()
	testRayService.Spec.ServeService = &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-serve-svc",
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{Name: "serve-grpc", Port: 9000, AppProtocol: &h2c},
				{Name: "not-a-serve-port", Port: 1234},
			},
		},
	}

	svc, err := BuildServeServiceForRayService(context.Background(), *testRayService, cluster)
	require.NoError(t, err)

	// All user-provided ports are kept as-is when no ports are auto-detected
	assert.Len(t, svc.Spec.Ports, 2)
	assert.Equal(t, "serve-grpc", svc.Spec.Ports[0].Name)
	assert.Equal(t, int32(9000), svc.Spec.Ports[0].Port)
	require.NotNil(t, svc.Spec.Ports[0].AppProtocol)
	assert.Equal(t, "kubernetes.io/h2c", *svc.Spec.Ports[0].AppProtocol)
	assert.Equal(t, "not-a-serve-port", svc.Spec.Ports[1].Name)
	assert.Equal(t, int32(1234), svc.Spec.Ports[1].Port)
}

func TestIsServingPort(t *testing.T) {
	assert.True(t, isServingPort("serve"))
	assert.True(t, isServingPort("serve-grpc"))
	assert.True(t, isServingPort("serve-http"))
	assert.False(t, isServingPort("server"))
	assert.False(t, isServingPort("dashboard"))
	assert.False(t, isServingPort("gcs-server"))
	assert.False(t, isServingPort("metrics"))
	assert.False(t, isServingPort(""))
}

func validateServiceTypeForUserSpecifiedService(svc *corev1.Service, userType corev1.ServiceType, t *testing.T) {
	// Test that the user service type takes priority over the default service type (example: ClusterIP)
	if svc.Spec.Type != userType {
		t.Errorf("Generated service type is not %s", userType)
	}
}

func validateNameAndNamespaceForUserSpecifiedService(svc *corev1.Service, defaultNamespace string, userName string, t *testing.T) {
	// Test name and namespace are generated if not specified
	if svc.ObjectMeta.Name == "" {
		t.Errorf("Generated service name is empty")
	}
	if svc.ObjectMeta.Namespace == "" {
		t.Errorf("Generated service namespace is empty")
	}
	// The user-provided namespace should be ignored, but the name should be respected
	if svc.ObjectMeta.Namespace != defaultNamespace {
		t.Errorf("User-provided namespace should be ignored: expected namespace=%s, actual namespace=%s", defaultNamespace, svc.ObjectMeta.Namespace)
	}
	if svc.ObjectMeta.Name != userName {
		t.Errorf("User-provided name should be respected: expected name=%s, actual name=%s", userName, svc.ObjectMeta.Name)
	}
}

func validateLabelsForUserSpecifiedService(svc *corev1.Service, userLabels map[string]string, t *testing.T) {
	for k := range userLabels {
		if _, ok := svc.ObjectMeta.Labels[k]; !ok {
			t.Errorf("Final labels should contain key=%s", k)
		}
	}
}
