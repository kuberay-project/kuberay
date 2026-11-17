package ray

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"

	cmap "github.com/orcaman/concurrent-map/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/lru"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	clientFake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	gwv1 "sigs.k8s.io/gateway-api/apis/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/dashboardclient"
	utiltypes "github.com/ray-project/kuberay/ray-operator/controllers/ray/utils/types"
	"github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/scheme"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
	"github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestGenerateHashWithoutReplicasAndWorkersToDelete(t *testing.T) {
	// `generateRayClusterJsonHash` will mute fields that will not trigger new RayCluster preparation. For example,
	// Autoscaler will update `Replicas` and `WorkersToDelete` when scaling up/down. Hence, `hash1` should be equal to
	// `hash2` in this case.
	cluster := rayv1.RayCluster{
		Spec: rayv1.RayClusterSpec{
			RayVersion: support.GetRayVersion(),
			WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
				{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{},
					},
					Replicas:    ptr.To[int32](2),
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: ptr.To[int32](4),
				},
			},
		},
	}

	hash1, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(cluster.Spec)
	require.NoError(t, err)

	*cluster.Spec.WorkerGroupSpecs[0].Replicas++
	hash2, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(cluster.Spec)
	require.NoError(t, err)
	assert.Equal(t, hash1, hash2)

	// RayVersion will not be muted, so `hash3` should not be equal to `hash1`.
	cluster.Spec.RayVersion = "2.100.0"
	hash3, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(cluster.Spec)
	require.NoError(t, err)
	assert.NotEqual(t, hash1, hash3)

	// Tolerations injected by external controllers (e.g., Kueue) should not change the hash.
	cluster.Spec.RayVersion = support.GetRayVersion()
	cluster.Spec.HeadGroupSpec.Template.Spec.Tolerations = []corev1.Toleration{
		{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	}
	cluster.Spec.WorkerGroupSpecs[0].Template.Spec.Tolerations = []corev1.Toleration{
		{Key: "nvidia.com/gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
	}
	hash4, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(cluster.Spec)
	require.NoError(t, err)
	assert.Equal(t, hash1, hash4)

	// SchedulingGates injected by external controllers (e.g., Kueue) should not change the hash.
	cluster.Spec.HeadGroupSpec.Template.Spec.Tolerations = nil
	cluster.Spec.WorkerGroupSpecs[0].Template.Spec.Tolerations = nil
	cluster.Spec.HeadGroupSpec.Template.Spec.SchedulingGates = []corev1.PodSchedulingGate{
		{Name: "kueue.x-k8s.io/admission"},
	}
	cluster.Spec.WorkerGroupSpecs[0].Template.Spec.SchedulingGates = []corev1.PodSchedulingGate{
		{Name: "kueue.x-k8s.io/admission"},
	}
	hash5, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(cluster.Spec)
	require.NoError(t, err)
	assert.Equal(t, hash1, hash5)
}

func TestIsHeadPodRunningAndReady(t *testing.T) {
	// Create a new scheme with CRDs, Pod, Service schemes.
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)

	// Mock data
	cluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: "default",
		},
	}

	headPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "head-pod",
			Namespace: cluster.ObjectMeta.Namespace,
			Labels: map[string]string{
				utils.RayClusterLabelKey:  cluster.ObjectMeta.Name,
				utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
			},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodScheduled,
					Status: corev1.ConditionTrue,
				},
			},
		},
	}

	// Initialize a fake client with newScheme and runtimeObjects.
	runtimeObjects := []runtime.Object{}
	fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()
	ctx := context.TODO()

	// Initialize RayService reconciler.
	r := &RayServiceReconciler{
		Client:   fakeClient,
		Recorder: &events.FakeRecorder{},
		Scheme:   scheme.Scheme,
	}

	// Test 1: There is no head pod. `isHeadPodRunningAndReady` should return false.
	// In addition, an error should be returned if the number of head pods is not 1.
	isReady, err := r.isHeadPodRunningAndReady(ctx, &cluster)
	require.Error(t, err)
	assert.False(t, isReady)

	// Test 2: There is one head pod, but the pod is not running and ready.
	// `isHeadPodRunningAndReady` should return false, and no error should be returned.
	runtimeObjects = []runtime.Object{headPod}
	fakeClient = clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()
	r.Client = fakeClient
	isReady, err = r.isHeadPodRunningAndReady(ctx, &cluster)
	require.NoError(t, err)
	assert.False(t, isReady)

	// Test 3: There is one head pod, and the pod is running and ready.
	// `isHeadPodRunningAndReady` should return true, and no error should be returned.
	runningHeadPod := headPod.DeepCopy()
	runningHeadPod.Status = corev1.PodStatus{
		Phase: corev1.PodRunning,
		Conditions: []corev1.PodCondition{
			{
				Type:   corev1.PodReady,
				Status: corev1.ConditionTrue,
			},
		},
	}
	runtimeObjects = []runtime.Object{runningHeadPod}
	fakeClient = clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()
	r.Client = fakeClient
	isReady, err = r.isHeadPodRunningAndReady(ctx, &cluster)
	require.NoError(t, err)
	assert.True(t, isReady)
}

func TestReconcileServices_UpdateService(t *testing.T) {
	// Create a new scheme with CRDs, Pod, Service schemes.
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)

	// Mock data
	namespace := "ray"
	cluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: namespace,
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{
								Name:  "ray-head",
								Ports: []corev1.ContainerPort{},
							},
						},
					},
				},
			},
		},
	}
	rayService := rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: cluster.ObjectMeta.Namespace,
		},
	}

	// Initialize a fake client with newScheme and runtimeObjects.
	runtimeObjects := []runtime.Object{}
	fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()

	// Initialize RayCluster reconciler.
	r := &RayServiceReconciler{
		Client:   fakeClient,
		Recorder: &events.FakeRecorder{},
		Scheme:   scheme.Scheme,
	}

	ctx := context.TODO()
	// Create a head service.
	_, err := r.reconcileServices(ctx, &rayService, &cluster, utils.HeadService)
	require.NoError(t, err, "Fail to reconcile service")

	svcList := corev1.ServiceList{}
	err = fakeClient.List(ctx, &svcList, client.InNamespace(namespace))
	require.NoError(t, err, "Fail to get service list")
	assert.Len(t, svcList.Items, 1, "Service list should have one item")
	oldSvc := svcList.Items[0].DeepCopy()

	// Test 1: When the service for the RayCluster already exists, it should not be updated.
	cluster.Spec.HeadGroupSpec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{
		{
			Name:          "test-port",
			ContainerPort: 9999,
		},
	}
	_, err = r.reconcileServices(ctx, &rayService, &cluster, utils.HeadService)
	require.NoError(t, err, "Fail to reconcile service")

	svcList = corev1.ServiceList{}
	err = fakeClient.List(ctx, &svcList, client.InNamespace(namespace))
	require.NoError(t, err, "Fail to get service list")
	assert.Len(t, svcList.Items, 1, "Service list should have one item")
	assert.True(t, reflect.DeepEqual(*oldSvc, svcList.Items[0]))

	// Test 2: When the RayCluster switches, the service should be updated.
	cluster.Name = "new-cluster"
	_, err = r.reconcileServices(ctx, &rayService, &cluster, utils.HeadService)
	require.NoError(t, err, "Fail to reconcile service")

	svcList = corev1.ServiceList{}
	err = fakeClient.List(ctx, &svcList, client.InNamespace(namespace))
	require.NoError(t, err, "Fail to get service list")
	assert.Len(t, svcList.Items, 1, "Service list should have one item")
	assert.False(t, reflect.DeepEqual(*oldSvc, svcList.Items[0]))
}

func TestFetchHeadServiceURL(t *testing.T) {
	// Create a new scheme with CRDs, Pod, Service schemes.
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)

	// Mock data
	namespace := "ray"
	dashboardPort := int32(9999)
	cluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cluster",
			Namespace: namespace,
		},
	}

	headSvcName, err := utils.GenerateHeadServiceName(utils.RayClusterCRD, cluster.Spec, cluster.Name)
	require.NoError(t, err, "Fail to generate head service name")
	headSvc := corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headSvcName,
			Namespace: cluster.ObjectMeta.Namespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Name: utils.DashboardPortName,
					Port: dashboardPort,
				},
			},
		},
	}

	// Initialize a fake client with newScheme and runtimeObjects.
	runtimeObjects := []runtime.Object{&headSvc}
	fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()

	// Initialize RayService reconciler.
	ctx := context.TODO()
	r := RayServiceReconciler{
		Client:   fakeClient,
		Recorder: &events.FakeRecorder{},
		Scheme:   scheme.Scheme,
	}

	url, err := utils.FetchHeadServiceURL(ctx, r.Client, &cluster, utils.DashboardPortName)
	require.NoError(t, err, "Fail to fetch head service url")
	assert.Equal(t, fmt.Sprintf("test-cluster-head-svc.%s.svc.cluster.local:%d", namespace, dashboardPort), url, "Head service url is not correct")
}

func TestGetAndCheckServeStatus(t *testing.T) {
	// Create a new scheme with CRDs, Pod, Service schemes.
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)

	// Initialize RayService reconciler.
	ctx := context.TODO()
	serveAppName := "serve-app-1"

	// Here are the key representing Ray Serve deployment and application statuses.
	const (
		// Ray Serve deployment status
		DeploymentStatus = "DeploymentStatus"
		// Ray Serve application status
		ApplicationStatus = "ApplicationStatus"
	)

	tests := []struct {
		rayServiceStatus map[string]string
		applications     map[string]rayv1.AppStatus
		name             string
		expectedReady    bool
	}{
		// Test 1: There is no pre-existing RayServiceStatus in the RayService CR. Create a new Ray Serve application, and the application is still deploying.
		{
			rayServiceStatus: map[string]string{
				DeploymentStatus:  rayv1.DeploymentStatusEnum.UPDATING,
				ApplicationStatus: rayv1.ApplicationStatusEnum.DEPLOYING,
			},
			applications:  map[string]rayv1.AppStatus{},
			name:          "Create a new Ray Serve application",
			expectedReady: false,
		},
		// Test 2: The Ray Serve application finishes the deployment process and becomes "RUNNING".
		{
			rayServiceStatus: map[string]string{
				DeploymentStatus:  rayv1.DeploymentStatusEnum.HEALTHY,
				ApplicationStatus: rayv1.ApplicationStatusEnum.RUNNING,
			},
			applications: map[string]rayv1.AppStatus{
				serveAppName: {
					Status: rayv1.ApplicationStatusEnum.RUNNING,
				},
			},
			name:          "Finishes the deployment process and becomes RUNNING",
			expectedReady: true,
		},
		// Test 3: Both the current Ray Serve application and RayService status are unhealthy.
		{
			rayServiceStatus: map[string]string{
				DeploymentStatus:  rayv1.DeploymentStatusEnum.UNHEALTHY,
				ApplicationStatus: rayv1.ApplicationStatusEnum.UNHEALTHY,
			},
			applications: map[string]rayv1.AppStatus{
				serveAppName: {
					Status: rayv1.ApplicationStatusEnum.UNHEALTHY,
				},
			},
			name:          "Both the current Ray Serve application and RayService status are unhealthy",
			expectedReady: false,
		},
		// Test 4: Both the current Ray Serve application and RayService status are DEPLOY_FAILED.
		{
			rayServiceStatus: map[string]string{
				DeploymentStatus:  rayv1.DeploymentStatusEnum.UPDATING,
				ApplicationStatus: rayv1.ApplicationStatusEnum.DEPLOY_FAILED,
			},
			applications: map[string]rayv1.AppStatus{
				serveAppName: {
					Status: rayv1.ApplicationStatusEnum.DEPLOY_FAILED,
				},
			},
			name:          "Both the current Ray Serve application and RayService status are DEPLOY_FAILED",
			expectedReady: false,
		},
		// Test 5: If the Ray Serve application is not found, the RayCluster is not ready to serve requests.
		{
			rayServiceStatus: map[string]string{},
			applications:     map[string]rayv1.AppStatus{},
			name:             "Ray Serve application is not found",
			expectedReady:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var dashboardClient dashboardclient.RayDashboardClientInterface
			if len(tc.rayServiceStatus) != 0 {
				dashboardClient = initFakeDashboardClient(serveAppName, tc.rayServiceStatus[DeploymentStatus], tc.rayServiceStatus[ApplicationStatus])
			} else {
				dashboardClient = &utils.FakeRayDashboardClient{}
			}
			isReady, _, err := getAndCheckServeStatus(ctx, dashboardClient)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedReady, isReady)
		})
	}
}

func TestCheckIfNeedSubmitServeApplications(t *testing.T) {
	serveConfigV2_1 := "serve-config-1"
	serveConfigV2_2 := "serve-config-2"

	serveApplications := map[string]rayv1.AppStatus{
		"myapp": {
			Status: rayv1.ApplicationStatusEnum.RUNNING,
		},
	}
	emptyServeApplications := map[string]rayv1.AppStatus{}

	// Test 1: The cached Serve config is empty, and the new Serve config is not empty.
	// This happens when the RayCluster is new, and the serve application has not been created yet.
	shouldCreate, _ := checkIfNeedSubmitServeApplications("", serveConfigV2_1, emptyServeApplications)
	assert.True(t, shouldCreate)

	// Test 2: The cached Serve config and the new Serve config are the same.
	// This happens when the serve application is already created, and users do not update the serve config.
	shouldCreate, _ = checkIfNeedSubmitServeApplications(serveConfigV2_1, serveConfigV2_1, serveApplications)
	assert.False(t, shouldCreate)

	// Test 3: The cached Serve config and the new Serve config are different.
	// This happens when the serve application is already created, and users update the serve config.
	shouldCreate, _ = checkIfNeedSubmitServeApplications(serveConfigV2_1, serveConfigV2_2, serveApplications)
	assert.True(t, shouldCreate)

	// Test 4: Both the cached Serve config and the new Serve config are the same, but the RayService CR status is empty.
	// This happens when the head Pod crashed and GCS FT was not enabled
	shouldCreate, _ = checkIfNeedSubmitServeApplications(serveConfigV2_1, serveConfigV2_1, emptyServeApplications)
	assert.True(t, shouldCreate)

	// Test 5: The cached Serve config is empty, but the new Serve config is not empty.
	// This happens when KubeRay operator crashes and restarts. Submit the request for safety.
	shouldCreate, _ = checkIfNeedSubmitServeApplications("", serveConfigV2_1, serveApplications)
	assert.True(t, shouldCreate)
}

func TestReconcileRayCluster_CreatePendingCluster(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)

	ctx := context.TODO()
	namespace := "ray"
	rayService := rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: namespace,
		},
		Status: rayv1.RayServiceStatuses{
			PendingServiceStatus: rayv1.RayServiceStatus{
				RayClusterName: "pending-cluster",
			},
		},
	}

	runtimeObjects := []runtime.Object{}
	fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()
	r := RayServiceReconciler{
		Client:   fakeClient,
		Scheme:   newScheme,
		Recorder: events.NewFakeRecorder(1),
	}

	activeRayCluster, pendingRayCluster, err := r.reconcileRayCluster(ctx, &rayService)
	require.NoError(t, err)
	assert.Nil(t, activeRayCluster)
	assert.Equal(t, "pending-cluster", pendingRayCluster.Name)
}

func TestReconcileRayCluster_UpdateActiveCluster(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)

	ctx := context.TODO()
	namespace := "ray"
	rayServiceTemplate := rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: namespace,
		},
		Status: rayv1.RayServiceStatuses{
			ActiveServiceStatus: rayv1.RayServiceStatus{
				RayClusterName: "active-cluster",
			},
		},
	}

	hash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(rayServiceTemplate.Spec.RayClusterSpec)
	require.NoError(t, err)
	activeClusterTemplate := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "active-cluster",
			Namespace: namespace,
			Annotations: map[string]string{
				utils.HashWithoutReplicasAndWorkersToDeleteKey: hash,
				utils.NumWorkerGroupsKey:                       strconv.Itoa(len(rayServiceTemplate.Spec.RayClusterSpec.WorkerGroupSpecs)),
				utils.KubeRayVersion:                           utils.KUBERAY_VERSION,
			},
		},
	}

	tests := []struct {
		name                 string
		updateKubeRayVersion bool
		addNewWorkerGroup    bool
	}{
		{
			name:                 "Update KubeRay version",
			updateKubeRayVersion: true,
			addNewWorkerGroup:    false,
		},
		{
			name:                 "Add new worker group",
			updateKubeRayVersion: false,
			addNewWorkerGroup:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cluster := activeClusterTemplate.DeepCopy()
			service := rayServiceTemplate.DeepCopy()
			if test.updateKubeRayVersion {
				cluster.Annotations[utils.KubeRayVersion] = "new-version"
			}
			if test.addNewWorkerGroup {
				service.Spec.RayClusterSpec.WorkerGroupSpecs = append(service.Spec.RayClusterSpec.WorkerGroupSpecs, rayv1.WorkerGroupSpec{
					GroupName: "new-worker-group",
				})
			}
			expectedWorkerGroupCount := len(service.Spec.RayClusterSpec.WorkerGroupSpecs)

			runtimeObjects := []runtime.Object{cluster}
			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()
			r := RayServiceReconciler{
				Client:   fakeClient,
				Scheme:   newScheme,
				Recorder: events.NewFakeRecorder(1),
			}

			activeCluster, pendingCluster, err := r.reconcileRayCluster(ctx, service)
			require.NoError(t, err)
			assert.Equal(t, cluster.Name, activeCluster.Name)
			assert.Nil(t, pendingCluster)

			if test.updateKubeRayVersion {
				assert.Equal(t, utils.KUBERAY_VERSION, activeCluster.Annotations[utils.KubeRayVersion])
			}
			assert.Len(t, activeCluster.Spec.WorkerGroupSpecs, expectedWorkerGroupCount)
		})
	}
}

func TestReconcileRayCluster_UpdatePendingCluster(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)

	ctx := context.TODO()
	namespace := "ray"
	rayServiceTemplate := rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: namespace,
		},
		Status: rayv1.RayServiceStatuses{
			PendingServiceStatus: rayv1.RayServiceStatus{
				RayClusterName: "pending-cluster",
			},
		},
	}

	hash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(rayServiceTemplate.Spec.RayClusterSpec)
	require.NoError(t, err)
	cluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pending-cluster",
			Namespace: namespace,
			Annotations: map[string]string{
				utils.HashWithoutReplicasAndWorkersToDeleteKey: hash,
				utils.NumWorkerGroupsKey:                       strconv.Itoa(len(rayServiceTemplate.Spec.RayClusterSpec.WorkerGroupSpecs)),
				utils.KubeRayVersion:                           utils.KUBERAY_VERSION,
			},
		},
	}

	service := rayServiceTemplate.DeepCopy()
	service.Spec.RayClusterSpec.WorkerGroupSpecs = append(service.Spec.RayClusterSpec.WorkerGroupSpecs, rayv1.WorkerGroupSpec{
		GroupName: "new-worker-group",
	})
	expectedWorkerGroupCount := len(service.Spec.RayClusterSpec.WorkerGroupSpecs)

	runtimeObjects := []runtime.Object{&cluster}
	fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()
	r := RayServiceReconciler{
		Client:   fakeClient,
		Scheme:   newScheme,
		Recorder: events.NewFakeRecorder(1),
	}

	activeCluster, pendingCluster, err := r.reconcileRayCluster(ctx, service)
	require.NoError(t, err)
	assert.Nil(t, activeCluster)
	assert.Equal(t, cluster.Name, pendingCluster.Name)
	assert.Len(t, pendingCluster.Spec.WorkerGroupSpecs, expectedWorkerGroupCount)
}

func initFakeDashboardClient(appName string, deploymentStatus string, appStatus string) dashboardclient.RayDashboardClientInterface {
	fakeDashboardClient := utils.FakeRayDashboardClient{}
	status := generateServeStatus(deploymentStatus, appStatus)
	fakeDashboardClient.SetMultiApplicationStatuses(map[string]*utiltypes.ServeApplicationStatus{appName: &status})
	return &fakeDashboardClient
}

func initFakeRayHttpProxyClient(isHealthy bool) utils.RayHttpProxyClientInterface {
	return &utils.FakeRayHttpProxyClient{
		IsHealthy: isHealthy,
	}
}

func TestLabelHeadPodForServeStatus(t *testing.T) {
	tests := []struct {
		name                       string
		expectServeResult          string
		excludeHeadPodFromServeSvc bool
		isHealthy                  bool
	}{
		{
			name:                       "Ray serve application is running, excludeHeadPodFromServeSvc is true",
			expectServeResult:          "false",
			excludeHeadPodFromServeSvc: true,
			isHealthy:                  true,
		},
		{
			name:                       "Ray serve application is running, excludeHeadPodFromServeSvc is false",
			expectServeResult:          "true",
			excludeHeadPodFromServeSvc: false,
			isHealthy:                  true,
		},
		{
			name:                       "Ray serve application is unhealthy, excludeHeadPodFromServeSvc is true",
			expectServeResult:          "false",
			excludeHeadPodFromServeSvc: true,
			isHealthy:                  false,
		},
		{
			name:                       "Ray serve application is unhealthy, excludeHeadPodFromServeSvc is false",
			expectServeResult:          "false",
			excludeHeadPodFromServeSvc: false,
			isHealthy:                  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			newScheme := runtime.NewScheme()
			_ = corev1.AddToScheme(newScheme)

			namespace := "mock-ray-namespace"
			cluster := rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-cluster",
					Namespace: namespace,
				},
			}
			headPod := &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "head-pod",
					Namespace: cluster.ObjectMeta.Namespace,
					Labels: map[string]string{
						utils.RayClusterLabelKey:  cluster.ObjectMeta.Name,
						utils.RayNodeTypeLabelKey: string(rayv1.HeadNode),
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name: "test-container",
						},
					},
				},
			}
			// Initialize a fake client with newScheme and runtimeObjects.
			runtimeObjects := []runtime.Object{headPod}
			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()
			ctx := context.TODO()

			fakeRayHttpProxyClient := initFakeRayHttpProxyClient(tc.isHealthy)
			// Initialize RayService reconciler.
			r := &RayServiceReconciler{
				Client:   fakeClient,
				Recorder: &events.FakeRecorder{},
				Scheme:   newScheme,
				httpProxyClientFunc: func(_, _, _ string, _ int) utils.RayHttpProxyClientInterface {
					return fakeRayHttpProxyClient
				},
			}

			err := r.updateHeadPodServeLabel(ctx, &rayv1.RayService{}, &cluster, tc.excludeHeadPodFromServeSvc)
			require.NoError(t, err)
			// Get latest headPod status
			headPod, err = common.GetRayClusterHeadPod(ctx, r, &cluster)
			assert.Equal(t, headPod.Labels[utils.RayClusterServingServiceLabelKey], tc.expectServeResult)
			require.NoError(t, err)
		})
	}
}

func TestCalculateConditions(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)

	tests := []struct {
		name                    string
		conditionType           rayv1.RayServiceConditionType
		originalConditionStatus metav1.ConditionStatus
		originalReason          string
		expectedConditionStatus metav1.ConditionStatus
		expectedReason          string
		rayServiceInstance      rayv1.RayService
	}{
		{
			name:                    "initial RayServiceReady",
			rayServiceInstance:      rayv1.RayService{},
			conditionType:           rayv1.RayServiceReady,
			originalConditionStatus: metav1.ConditionFalse,
			originalReason:          string(rayv1.RayServiceInitializing),
			expectedConditionStatus: metav1.ConditionFalse,
			expectedReason:          string(rayv1.RayServiceInitializing),
		},
		{
			name:                    "initial RayServiceInitializing",
			rayServiceInstance:      rayv1.RayService{},
			conditionType:           rayv1.UpgradeInProgress,
			originalConditionStatus: metav1.ConditionFalse,
			originalReason:          string(rayv1.RayServiceInitializing),
			expectedConditionStatus: metav1.ConditionFalse,
			expectedReason:          string(rayv1.RayServiceInitializing),
		},
		{
			name: "Ready condition remains false unchanged",
			rayServiceInstance: rayv1.RayService{
				Status: rayv1.RayServiceStatuses{
					NumServeEndpoints: 0,
				},
			},
			conditionType:           rayv1.RayServiceReady,
			originalConditionStatus: metav1.ConditionFalse,
			originalReason:          "WhateverReason",
			expectedConditionStatus: metav1.ConditionFalse,
			expectedReason:          "WhateverReason",
		},
		{
			name: "Ready condition remains true always has NonZeroServeEndPoints reason",
			rayServiceInstance: rayv1.RayService{
				Status: rayv1.RayServiceStatuses{
					NumServeEndpoints: 1,
				},
			},
			conditionType:           rayv1.RayServiceReady,
			originalConditionStatus: metav1.ConditionTrue,
			originalReason:          "WhateverReason",
			expectedConditionStatus: metav1.ConditionTrue,
			expectedReason:          string(rayv1.NonZeroServeEndpoints),
		},
		{
			name: "Ready condition becomes true",
			rayServiceInstance: rayv1.RayService{
				Status: rayv1.RayServiceStatuses{
					NumServeEndpoints: 1,
				},
			},
			conditionType:           rayv1.RayServiceReady,
			originalConditionStatus: metav1.ConditionFalse,
			originalReason:          "WhateverReason",
			expectedConditionStatus: metav1.ConditionTrue,
			expectedReason:          string(rayv1.NonZeroServeEndpoints),
		},
		{
			name: "Ready condition becomes false",
			rayServiceInstance: rayv1.RayService{
				Status: rayv1.RayServiceStatuses{
					NumServeEndpoints: 0,
				},
			},
			conditionType:           rayv1.RayServiceReady,
			originalConditionStatus: metav1.ConditionTrue,
			originalReason:          string(rayv1.NonZeroServeEndpoints),
			expectedConditionStatus: metav1.ConditionFalse,
			expectedReason:          string(rayv1.ZeroServeEndpoints),
		},
		{
			name: "UpgradeInProgress condition is true if both active and pending clusters exist",
			rayServiceInstance: rayv1.RayService{
				Status: rayv1.RayServiceStatuses{
					ActiveServiceStatus: rayv1.RayServiceStatus{
						RayClusterName: "active-cluster",
					},
					PendingServiceStatus: rayv1.RayServiceStatus{
						RayClusterName: "pending-cluster",
					},
				},
			},
			conditionType:           rayv1.UpgradeInProgress,
			originalConditionStatus: metav1.ConditionFalse,
			originalReason:          "WhateverReason",
			expectedConditionStatus: metav1.ConditionTrue,
			expectedReason:          string(rayv1.BothActivePendingClustersExist),
		},
		{
			name: "UpgradeInProgress condition is false if only active cluster exists",
			rayServiceInstance: rayv1.RayService{
				Status: rayv1.RayServiceStatuses{
					ActiveServiceStatus: rayv1.RayServiceStatus{
						RayClusterName: "active-cluster",
					},
				},
			},
			conditionType:           rayv1.UpgradeInProgress,
			originalConditionStatus: metav1.ConditionTrue,
			originalReason:          string(rayv1.BothActivePendingClustersExist),
			expectedConditionStatus: metav1.ConditionFalse,
			expectedReason:          string(rayv1.NoPendingCluster),
		},
		{
			name:                    "UpgradeInProgress condition is unknown if no active cluster exists and RayService is not initializing",
			rayServiceInstance:      rayv1.RayService{},
			conditionType:           rayv1.UpgradeInProgress,
			originalConditionStatus: metav1.ConditionTrue,
			originalReason:          string(rayv1.BothActivePendingClustersExist),
			expectedConditionStatus: metav1.ConditionUnknown,
			expectedReason:          string(rayv1.NoActiveCluster),
		},
		{
			name: "UpgradeInProgress condition is false if RayService is initializing",
			rayServiceInstance: rayv1.RayService{
				Status: rayv1.RayServiceStatuses{
					PendingServiceStatus: rayv1.RayServiceStatus{
						RayClusterName: "pending-cluster",
					},
				},
			},
			conditionType:           rayv1.UpgradeInProgress,
			originalConditionStatus: metav1.ConditionFalse,
			originalReason:          string(rayv1.RayServiceInitializing),
			expectedConditionStatus: metav1.ConditionFalse,
			expectedReason:          string(rayv1.RayServiceInitializing),
		},
		{
			name: "UpgradeInProgress condition is not overridden when in InitializingTimeout state with no active cluster",
			rayServiceInstance: rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 1,
				},
				Status: rayv1.RayServiceStatuses{
					NumServeEndpoints:  0,
					ObservedGeneration: 1,
					// No active cluster, no pending cluster
					// This triggers the line 351-355 logic in calculateConditions
				},
			},
			conditionType:           rayv1.UpgradeInProgress,
			originalConditionStatus: metav1.ConditionFalse,
			originalReason:          string(rayv1.RayServiceInitializingTimeout),
			expectedConditionStatus: metav1.ConditionFalse,
			expectedReason:          string(rayv1.RayServiceInitializingTimeout),
		},
		{
			name: "Conditions calculated normally when not in InitializingTimeout state",
			rayServiceInstance: rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{
					Generation: 1,
				},
				Status: rayv1.RayServiceStatuses{
					NumServeEndpoints:  5,
					ObservedGeneration: 1,
					ActiveServiceStatus: rayv1.RayServiceStatus{
						RayClusterName: "active",
					},
				},
			},
			conditionType:           rayv1.RayServiceReady,
			originalConditionStatus: metav1.ConditionFalse,
			originalReason:          string(rayv1.RayServiceInitializing),
			expectedConditionStatus: metav1.ConditionTrue,
			expectedReason:          string(rayv1.NonZeroServeEndpoints),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).Build()
			fakeRecorder := events.NewFakeRecorder(10)

			r := &RayServiceReconciler{
				Client:   fakeClient,
				Scheme:   scheme.Scheme,
				Recorder: fakeRecorder,
			}

			meta.SetStatusCondition(&tt.rayServiceInstance.Status.Conditions, metav1.Condition{
				Type:               string(tt.conditionType),
				Status:             tt.originalConditionStatus,
				Reason:             tt.originalReason,
				ObservedGeneration: tt.rayServiceInstance.ObjectMeta.Generation,
			})
			calculateConditions(ctx, r, &tt.rayServiceInstance)
			condition := meta.FindStatusCondition(tt.rayServiceInstance.Status.Conditions, string(tt.conditionType))
			assert.Equal(t, tt.expectedConditionStatus, condition.Status)
			assert.Equal(t, tt.expectedReason, condition.Reason)
		})
	}
}

func TestConstructRayClusterForRayService(t *testing.T) {
	tests := []struct {
		name       string
		rayService rayv1.RayService
	}{
		{
			name: "RayClusterSpec with no worker groups",
			rayService: rayv1.RayService{
				Spec: rayv1.RayServiceSpec{
					RayClusterSpec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{},
					},
				},
			},
		},
		{
			name: "RayClusterSpec with two worker groups",
			rayService: rayv1.RayService{
				Spec: rayv1.RayServiceSpec{
					RayClusterSpec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "worker-group-1",
							},
							{
								GroupName: "worker-group-2",
							},
						},
					},
				},
			},
		},
		{
			name: "RayService with labels",
			rayService: rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"label-1": "value-1",
						"label-2": "value-2",
					},
				},
				Spec: rayv1.RayServiceSpec{
					RayClusterSpec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "worker-group-1",
							},
						},
					},
				},
			},
		},
		{
			name: "RayService with annotations",
			rayService: rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						"annotation-1": "value-1",
						"annotation-2": "value-2",
					},
				},
				Spec: rayv1.RayServiceSpec{
					RayClusterSpec: rayv1.RayClusterSpec{
						WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
							{
								GroupName: "worker-group-1",
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rayService := tt.rayService
			rayService.Name = "test-service"
			rayService.Namespace = "test-namespace"
			clusterName := "test-cluster"
			rayCluster, err := constructRayClusterForRayService(&rayService, clusterName, scheme.Scheme)
			require.NoError(t, err)

			// Check ObjectMeta of the RayCluster
			assert.Equal(t, rayCluster.ObjectMeta.Name, clusterName)
			assert.Equal(t, rayCluster.ObjectMeta.Namespace, rayService.Namespace)

			// Check labels for metadata
			assert.Equal(t, rayCluster.Labels[utils.RayOriginatedFromCRNameLabelKey], rayService.Name)
			assert.Equal(t, rayCluster.Labels[utils.RayOriginatedFromCRDLabelKey], string(utils.RayServiceCRD))

			// Check annotations for metadata
			assert.NotEmpty(t, rayCluster.Annotations[utils.HashWithoutReplicasAndWorkersToDeleteKey])
			expectedNumWorkerGroups := strconv.Itoa(len(rayService.Spec.RayClusterSpec.WorkerGroupSpecs))
			assert.Equal(t, expectedNumWorkerGroups, rayCluster.Annotations[utils.NumWorkerGroupsKey])
			assert.Equal(t, utils.KUBERAY_VERSION, rayCluster.Annotations[utils.KubeRayVersion])

			// Check whether the RayService's labels are copied to the RayCluster
			for key, value := range rayService.Labels {
				assert.Equal(t, rayCluster.Labels[key], value)
			}

			// Check whether the RayService's annotations are copied to the RayCluster
			for key, value := range rayService.Annotations {
				assert.Equal(t, rayCluster.Annotations[key], value)
			}

			// Check owner reference
			assert.Equal(t, rayCluster.OwnerReferences[0].Name, rayService.Name)
			assert.Equal(t, rayCluster.OwnerReferences[0].UID, rayService.UID)
		})
	}
}

func TestIsClusterSpecHashEqual(t *testing.T) {
	rayService := rayv1.RayService{
		Spec: rayv1.RayServiceSpec{
			RayClusterSpec: rayv1.RayClusterSpec{
				WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
					{
						GroupName: "worker-group-1",
						Replicas:  ptr.To[int32](1),
					},
				},
			},
		},
	}

	tests := []struct {
		name              string
		partial           bool
		diffReplicas      bool
		expected          bool
		addNewWorkerGroup bool
		updateClusterSpec bool
	}{
		{
			name:              "[full] diff replicas",
			partial:           false,
			diffReplicas:      true,
			addNewWorkerGroup: false,
			expected:          true,
		},
		{
			name:              "[full] completely identical",
			partial:           false,
			diffReplicas:      false,
			addNewWorkerGroup: false,
			expected:          true,
		},
		{
			name:              "[full] update cluster spec",
			partial:           false,
			diffReplicas:      false,
			addNewWorkerGroup: false,
			updateClusterSpec: true,
			expected:          false,
		},
		{
			name:              "[partial] new worker group",
			partial:           true,
			diffReplicas:      false,
			addNewWorkerGroup: true,
			expected:          true,
		},
		{
			name:              "[partial] diff replicas + new worker group",
			partial:           true,
			diffReplicas:      true,
			addNewWorkerGroup: true,
			expected:          true,
		},
		{
			name:              "[partial] diff replicas",
			partial:           true,
			diffReplicas:      true,
			addNewWorkerGroup: false,
			expected:          true,
		},
		{
			name:              "[partial] update cluster spec",
			partial:           true,
			updateClusterSpec: true,
			expected:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := rayService.DeepCopy()
			hash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(service.Spec.RayClusterSpec)
			require.NoError(t, err)
			cluster := rayv1.RayCluster{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						utils.HashWithoutReplicasAndWorkersToDeleteKey: hash,
						utils.NumWorkerGroupsKey:                       strconv.Itoa(len(rayService.Spec.RayClusterSpec.WorkerGroupSpecs)),
					},
				},
				Spec: rayService.Spec.RayClusterSpec,
			}
			if tt.diffReplicas {
				*service.Spec.RayClusterSpec.WorkerGroupSpecs[0].Replicas++
			}
			if tt.addNewWorkerGroup {
				service.Spec.RayClusterSpec.WorkerGroupSpecs = append(service.Spec.RayClusterSpec.WorkerGroupSpecs, rayv1.WorkerGroupSpec{
					GroupName: "worker-group-2",
					Replicas:  ptr.To[int32](1),
				})
			}
			if tt.updateClusterSpec {
				service.Spec.RayClusterSpec.RayVersion = "new-version"
			}

			isEqual := isClusterSpecHashEqual(service, &cluster, tt.partial)
			assert.Equal(t, tt.expected, isEqual)
		})
	}
}

func TestShouldPrepareNewCluster_PrepareNewCluster(t *testing.T) {
	// Prepare a new cluster when both active and pending clusters are nil.
	ctx := context.TODO()
	rayService := rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: "test-namespace",
		},
	}

	shouldPrepareNewCluster := shouldPrepareNewCluster(ctx, &rayService, nil, nil, false)
	assert.True(t, shouldPrepareNewCluster)
}

func TestShouldPrepareNewCluster_ZeroDowntimeUpgrade(t *testing.T) {
	// Trigger a zero-downtime upgrade when the cluster spec in RayService differs
	// from the active cluster and no pending cluster exists.
	ctx := context.TODO()
	namespace := "test-namespace"
	activeClusterName := "active-cluster"

	rayService := rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: namespace,
		},
		Spec: rayv1.RayServiceSpec{
			RayClusterSpec: rayv1.RayClusterSpec{
				RayVersion: "old-version",
			},
		},
	}

	hash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(rayService.Spec.RayClusterSpec)
	require.NoError(t, err)
	activeCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      activeClusterName,
			Namespace: namespace,
			Annotations: map[string]string{
				utils.HashWithoutReplicasAndWorkersToDeleteKey: hash,
				utils.NumWorkerGroupsKey:                       strconv.Itoa(len(rayService.Spec.RayClusterSpec.WorkerGroupSpecs)),
				utils.KubeRayVersion:                           utils.KUBERAY_VERSION,
			},
		},
	}

	// Update cluster spec in RayService to trigger a zero downtime upgrade.
	rayService.Spec.RayClusterSpec.RayVersion = "new-version"
	shouldPrepareNewCluster := shouldPrepareNewCluster(ctx, &rayService, activeCluster, nil, false)
	assert.True(t, shouldPrepareNewCluster)
}

func TestShouldPrepareNewCluster_PendingCluster(t *testing.T) {
	// A new cluster will not be created if the K8s services are pointing to the pending cluster.
	ctx := context.TODO()
	namespace := "test-namespace"
	pendingClusterName := "pending-cluster"

	rayService := rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-service",
			Namespace: namespace,
		},
		Spec: rayv1.RayServiceSpec{
			RayClusterSpec: rayv1.RayClusterSpec{
				RayVersion: "old-version",
			},
		},
	}

	hash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(rayService.Spec.RayClusterSpec)
	require.NoError(t, err)
	pendingCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pendingClusterName,
			Namespace: namespace,
			Annotations: map[string]string{
				utils.HashWithoutReplicasAndWorkersToDeleteKey: hash,
				utils.NumWorkerGroupsKey:                       strconv.Itoa(len(rayService.Spec.RayClusterSpec.WorkerGroupSpecs)),
				utils.KubeRayVersion:                           utils.KUBERAY_VERSION,
			},
		},
	}
	rayService.Spec.RayClusterSpec.RayVersion = "new-version"

	t.Run("override the pending cluster if it is not serving", func(t *testing.T) {
		shouldPrepareNewCluster := shouldPrepareNewCluster(ctx, &rayService, nil, pendingCluster, false)
		assert.True(t, shouldPrepareNewCluster)
	})

	t.Run("do not override the pending cluster if it is serving", func(t *testing.T) {
		shouldPrepareNewCluster := shouldPrepareNewCluster(ctx, &rayService, nil, pendingCluster, true)
		assert.False(t, shouldPrepareNewCluster)
	})
}

func TestIsZeroDowntimeUpgradeEnabled(t *testing.T) {
	tests := []struct {
		name                     string
		upgradeStrategy          *rayv1.RayServiceUpgradeStrategy
		enableZeroDowntimeEnvVar string // "true" or "false" or "" (not set)
		expected                 bool
	}{
		{
			// The most common case.
			name:                     "both upgrade strategy and env var are not set",
			upgradeStrategy:          nil,
			enableZeroDowntimeEnvVar: "",
			expected:                 true,
		},
		{
			name:                     "upgrade strategy is not set, but env var is set to true",
			upgradeStrategy:          nil,
			enableZeroDowntimeEnvVar: "true",
			expected:                 true,
		},
		{
			name:                     "upgrade strategy is not set, but env var is set to false",
			upgradeStrategy:          nil,
			enableZeroDowntimeEnvVar: "false",
			expected:                 false,
		},
		{
			name:                     "upgrade strategy is set to NewCluster",
			upgradeStrategy:          &rayv1.RayServiceUpgradeStrategy{Type: ptr.To(rayv1.RayServiceNewCluster)},
			enableZeroDowntimeEnvVar: "",
			expected:                 true,
		},
		{
			name:                     "upgrade strategy is set to NewCluster, and env var is not set",
			upgradeStrategy:          &rayv1.RayServiceUpgradeStrategy{Type: ptr.To(rayv1.RayServiceNewCluster)},
			enableZeroDowntimeEnvVar: "true",
			expected:                 true,
		},
		{
			name:                     "upgrade strategy is set to NewCluster, and env var is set to false",
			upgradeStrategy:          &rayv1.RayServiceUpgradeStrategy{Type: ptr.To(rayv1.RayServiceNewCluster)},
			enableZeroDowntimeEnvVar: "false",
			expected:                 true,
		},
		{
			name:                     "upgrade strategy is set to RayServiceUpgradeNone, and env var is not set",
			upgradeStrategy:          &rayv1.RayServiceUpgradeStrategy{Type: ptr.To(rayv1.RayServiceUpgradeNone)},
			enableZeroDowntimeEnvVar: "",
			expected:                 false,
		},
		{
			name:                     "upgrade strategy is set to RayServiceUpgradeNone, and env var is set to true",
			upgradeStrategy:          &rayv1.RayServiceUpgradeStrategy{Type: ptr.To(rayv1.RayServiceUpgradeNone)},
			enableZeroDowntimeEnvVar: "true",
			expected:                 false,
		},
		{
			name:                     "upgrade strategy is set to RayServiceUpgradeNone, and env var is set to false",
			upgradeStrategy:          &rayv1.RayServiceUpgradeStrategy{Type: ptr.To(rayv1.RayServiceUpgradeNone)},
			enableZeroDowntimeEnvVar: "false",
			expected:                 false,
		},
	}

	ctx := context.TODO()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				os.Unsetenv(ENABLE_ZERO_DOWNTIME)
			}()

			os.Setenv(ENABLE_ZERO_DOWNTIME, tt.enableZeroDowntimeEnvVar)
			isEnabled := isZeroDowntimeUpgradeEnabled(ctx, tt.upgradeStrategy)
			assert.Equal(t, tt.expected, isEnabled)
		})
	}
}

func TestRayClusterDeletionDelaySeconds(t *testing.T) {
	namespace := "test-namespace"
	rayClusterName := "test-cluster"
	rayServiceName := "test-rayservice"

	// Helper to create a RayService with optional RayClusterDeletionDelaySeconds
	createRayService := func(delaySeconds *int32) *rayv1.RayService {
		return &rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rayServiceName,
				Namespace: namespace,
			},
			Spec: rayv1.RayServiceSpec{
				RayClusterDeletionDelaySeconds: delaySeconds,
			},
		}
	}

	rayCluster := rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      rayClusterName,
			Namespace: namespace,
			Labels: map[string]string{
				utils.RayOriginatedFromCRNameLabelKey: rayServiceName,
				utils.RayOriginatedFromCRDLabelKey:    utils.RayOriginatedFromCRDLabelValue(utils.RayServiceCRD),
			},
		},
	}

	tests := []struct {
		delaySeconds     *int32
		name             string
		expectedDuration time.Duration
	}{
		{
			name:             "Use default delay when not set",
			delaySeconds:     nil,
			expectedDuration: RayClusterDeletionDelayDuration,
		},
		{
			name:             "Use custom delay when set to 0",
			delaySeconds:     ptr.To[int32](0),
			expectedDuration: 0 * time.Second,
		},
		{
			name:             "Use custom delay when set to positive",
			delaySeconds:     ptr.To[int32](5),
			expectedDuration: 5 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			newScheme := runtime.NewScheme()
			_ = rayv1.AddToScheme(newScheme)
			ctx := context.TODO()

			rayService := createRayService(tc.delaySeconds)

			// Initialize a fake client with newScheme and runtimeObjects.
			runtimeObjects := []runtime.Object{rayService, &rayCluster}
			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()
			r := RayServiceReconciler{
				Client:                       fakeClient,
				Scheme:                       newScheme,
				Recorder:                     events.NewFakeRecorder(1),
				RayClusterDeletionTimestamps: cmap.New[time.Time](),
			}

			now := time.Now()
			_, err := r.cleanUpRayClusterInstance(ctx, rayService)
			require.NoError(t, err)

			// Check that the deletion timestamp is set and equals to the expected value
			ts, exists := r.RayClusterDeletionTimestamps.Get(rayClusterName)
			assert.True(t, exists, "Deletion timestamp should be set for the cluster")
			expectedTs := now.Add(tc.expectedDuration)

			assert.InDelta(t, expectedTs.Unix(), ts.Unix(), 1, "Deletion timestamp should be within 1 second of expected timestamp")
		})
	}
}

// Helper function to create a RayService object undergoing an incremental upgrade.
func makeIncrementalUpgradeRayService(
	withOptions bool,
	gatewayClassName string,
	stepSizePercent *int32,
	intervalSeconds *int32,
	routedPercent *int32,
	lastTrafficMigratedTime *metav1.Time,
) *rayv1.RayService {
	spec := rayv1.RayServiceSpec{
		ServeService: &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "serve-service",
				Namespace: "test-ns",
			},
			Spec: corev1.ServiceSpec{
				Ports: []corev1.ServicePort{
					{
						Name: "http",
						Port: 8000,
					},
				},
			},
		},
	}
	if withOptions {
		spec.UpgradeStrategy = &rayv1.RayServiceUpgradeStrategy{
			Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
			ClusterUpgradeOptions: &rayv1.ClusterUpgradeOptions{
				GatewayClassName: gatewayClassName,
				StepSizePercent:  stepSizePercent,
				IntervalSeconds:  intervalSeconds,
			},
		}
	}

	return &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "incremental-ray-service",
			Namespace: "test-ns",
		},
		Spec: spec,
		Status: rayv1.RayServiceStatuses{
			ActiveServiceStatus: rayv1.RayServiceStatus{
				RayClusterName: "active-ray-cluster",
				RayClusterStatus: rayv1.RayClusterStatus{
					Head: rayv1.HeadInfo{ServiceName: "active-service"},
				},
				TrafficRoutedPercent:    routedPercent,
				LastTrafficMigratedTime: lastTrafficMigratedTime,
			},
			PendingServiceStatus: rayv1.RayServiceStatus{
				RayClusterName: "pending-ray-cluster",
				RayClusterStatus: rayv1.RayClusterStatus{
					Head: rayv1.HeadInfo{ServiceName: "pending-service"},
				},
				TrafficRoutedPercent:    new(int32(100) - *routedPercent),
				LastTrafficMigratedTime: lastTrafficMigratedTime,
			},
		},
	}
}

func TestCreateGateway(t *testing.T) {
	serveService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "serve-service",
			Namespace: "test-ns",
		},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{
				{
					Port: 8000,
				},
			},
		},
	}
	newScheme := runtime.NewScheme()
	_ = corev1.AddToScheme(newScheme)

	fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(serveService).Build()
	reconciler := &RayServiceReconciler{
		Client: fakeClient,
	}

	tests := []struct {
		rayService          *rayv1.RayService
		name                string
		expectedGatewayName string
		expectedClass       string
		expectedListeners   int
		expectErr           bool
	}{
		{
			name:                "valid gateway creation",
			expectedGatewayName: "incremental-ray-service-gateway",
			rayService:          makeIncrementalUpgradeRayService(true, "gateway-class", new(int32(50)), new(int32(10)), new(int32(80)), &metav1.Time{Time: time.Now()}),
			expectErr:           false,
			expectedClass:       "gateway-class",
			expectedListeners:   1,
		},
		{
			name:       "missing ClusterUpgradeOptions",
			rayService: makeIncrementalUpgradeRayService(false, "gateway-class", new(int32(0)), new(int32(0)), new(int32(0)), &metav1.Time{Time: time.Now()}),
			expectErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gw, err := reconciler.createGateway(tt.rayService)
			if tt.expectErr {
				require.Error(t, err)
				assert.Nil(t, gw)
			} else {
				require.NoError(t, err)
				require.NotNil(t, gw)
				assert.Equal(t, tt.expectedGatewayName, gw.Name)
				assert.Equal(t, tt.rayService.Namespace, gw.Namespace)
				assert.Equal(t, gwv1.ObjectName(tt.expectedClass), gw.Spec.GatewayClassName)
				assert.Len(t, gw.Spec.Listeners, tt.expectedListeners)
			}
		})
	}
}

// When GatewayRef is set, reconcileGateway must NOT create a per-RayService
// Gateway. If the referenced (shared) Gateway exists it reconciles cleanly; if it
// is missing it records an event and returns without creating anything.
func TestReconcileGatewayWithGatewayRef(t *testing.T) {
	ctx := context.TODO()
	rsNamespace := "test-ns"
	gwNamespace := "gateways"

	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = gwv1.Install(newScheme)

	makeRS := func() *rayv1.RayService {
		rs := makeIncrementalUpgradeRayService(true, "", new(int32(50)), new(int32(10)), new(int32(80)), &metav1.Time{Time: time.Now()})
		rs.Namespace = rsNamespace
		rs.Spec.UpgradeStrategy.ClusterUpgradeOptions.GatewayRef = &rayv1.GatewayReference{
			Name:      "shared-gateway",
			Namespace: gwNamespace,
		}
		return rs
	}
	perServiceGatewayName := types.NamespacedName{Name: "incremental-ray-service-gateway", Namespace: rsNamespace}

	t.Run("referenced Gateway exists: reconcile succeeds, no per-service Gateway created", func(t *testing.T) {
		rayService := makeRS()
		sharedGateway := &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "shared-gateway", Namespace: gwNamespace}}
		fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).
			WithRuntimeObjects(sharedGateway, rayService).Build()
		reconciler := &RayServiceReconciler{Client: fakeClient, Scheme: newScheme, Recorder: events.NewFakeRecorder(10)}

		require.NoError(t, reconciler.reconcileGateway(ctx, rayService))

		err := fakeClient.Get(ctx, perServiceGatewayName, &gwv1.Gateway{})
		assert.True(t, errors.IsNotFound(err), "KubeRay must not create a per-RayService Gateway when GatewayRef is set")
	})

	t.Run("referenced Gateway missing: no error, records event, creates nothing", func(t *testing.T) {
		rayService := makeRS()
		fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(rayService).Build()
		reconciler := &RayServiceReconciler{Client: fakeClient, Scheme: newScheme, Recorder: events.NewFakeRecorder(10)}

		require.NoError(t, reconciler.reconcileGateway(ctx, rayService))

		err := fakeClient.Get(ctx, perServiceGatewayName, &gwv1.Gateway{})
		assert.True(t, errors.IsNotFound(err), "no Gateway should be created when the referenced Gateway is missing")
	})

	t.Run("referenced Gateway spec is left unchanged", func(t *testing.T) {
		rayService := makeRS()
		// A shared Gateway with its own class and multiple listeners, none of which
		// match what KubeRay would configure on a Gateway it creates itself.
		sharedGateway := &gwv1.Gateway{
			ObjectMeta: metav1.ObjectMeta{Name: "shared-gateway", Namespace: gwNamespace},
			Spec: gwv1.GatewaySpec{
				GatewayClassName: gwv1.ObjectName("contour"),
				Listeners: []gwv1.Listener{
					{Name: "http", Protocol: gwv1.HTTPProtocolType, Port: 80},
					{Name: "https", Protocol: gwv1.HTTPSProtocolType, Port: 443},
				},
			},
		}
		wantSpec := *sharedGateway.Spec.DeepCopy()
		fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).
			WithRuntimeObjects(sharedGateway, rayService).Build()
		reconciler := &RayServiceReconciler{Client: fakeClient, Scheme: newScheme, Recorder: events.NewFakeRecorder(10)}

		require.NoError(t, reconciler.reconcileGateway(ctx, rayService))

		got := &gwv1.Gateway{}
		require.NoError(t, fakeClient.Get(ctx, types.NamespacedName{Name: sharedGateway.Name, Namespace: gwNamespace}, got))
		assert.Equal(t, wantSpec, got.Spec, "reconcileGateway must not modify the referenced Gateway's spec")
	})
}

// Suspending a RayService that attaches to a shared Gateway via GatewayRef
// must NOT delete that shared Gateway (KubeRay does not own it — deleting it
// would break ingress for every other RayService on it). It must still delete
// the KubeRay-created per-RayService Gateway when this RayService controls it
// (e.g. left behind after switching from gatewayClassName to gatewayRef), but
// never a same-named Gateway it does not control. The KubeRay-owned HTTPRoute
// in the RayService namespace must always be deleted.
func TestDeleteRayServiceOwnedResourcesPreservesExistingGateway(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)

	ctx := context.TODO()
	rsNamespace := "test-ns"
	gwNamespace := "gateways"

	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)
	_ = gwv1.Install(newScheme)

	makeObjects := func(controlled bool) (*rayv1.RayService, *gwv1.Gateway, *gwv1.Gateway, *gwv1.HTTPRoute) {
		sharedGateway := &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "shared-gateway", Namespace: gwNamespace}}
		rayService := &rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{Name: "test-rayservice", Namespace: rsNamespace, UID: "rs-uid"},
			Spec: rayv1.RayServiceSpec{
				UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
					Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
					ClusterUpgradeOptions: &rayv1.ClusterUpgradeOptions{
						StepSizePercent: new(int32(10)),
						IntervalSeconds: new(int32(30)),
						GatewayRef: &rayv1.GatewayReference{
							Name:      sharedGateway.Name,
							Namespace: sharedGateway.Namespace,
						},
					},
				},
			},
		}
		// Per-RayService Gateway left behind after switching this RayService from
		// gatewayClassName to gatewayRef.
		ownedGateway := &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{
			Name:      rayService.Name + "-gateway",
			Namespace: rsNamespace,
		}}
		if controlled {
			ownedGateway.OwnerReferences = []metav1.OwnerReference{{
				APIVersion: rayv1.SchemeGroupVersion.String(),
				Kind:       "RayService",
				Name:       rayService.Name,
				UID:        rayService.UID,
				Controller: new(true),
			}}
		}
		httpRoute := &gwv1.HTTPRoute{ObjectMeta: metav1.ObjectMeta{
			Name:      rayService.Name + "-httproute",
			Namespace: rsNamespace,
		}}
		return rayService, sharedGateway, ownedGateway, httpRoute
	}

	run := func(t *testing.T, controlled bool) (client.Client, *rayv1.RayService, *gwv1.Gateway, *gwv1.Gateway, *gwv1.HTTPRoute) {
		rayService, sharedGateway, ownedGateway, httpRoute := makeObjects(controlled)
		fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).
			WithRuntimeObjects(sharedGateway, ownedGateway, httpRoute, rayService).Build()
		reconciler := &RayServiceReconciler{
			Client:   fakeClient,
			Scheme:   newScheme,
			Recorder: events.NewFakeRecorder(10),
		}
		_, err := reconciler.deleteRayServiceOwnedResources(ctx, rayService)
		require.NoError(t, err)

		// The shared Gateway must still exist in both cases.
		err = fakeClient.Get(ctx, types.NamespacedName{Name: sharedGateway.Name, Namespace: gwNamespace}, &gwv1.Gateway{})
		require.NoError(t, err, "shared Gateway must NOT be deleted on suspend when GatewayRef is set")

		// The KubeRay-owned HTTPRoute must be deleted in both cases.
		err = fakeClient.Get(ctx, types.NamespacedName{Name: httpRoute.Name, Namespace: rsNamespace}, &gwv1.HTTPRoute{})
		assert.True(t, errors.IsNotFound(err), "HTTPRoute should be deleted on suspend, got err=%v", err)

		return fakeClient, rayService, sharedGateway, ownedGateway, httpRoute
	}

	t.Run("Gateway controlled by the RayService is deleted", func(t *testing.T) {
		fakeClient, _, _, ownedGateway, _ := run(t, true)
		err := fakeClient.Get(ctx, types.NamespacedName{Name: ownedGateway.Name, Namespace: rsNamespace}, &gwv1.Gateway{})
		assert.True(t, errors.IsNotFound(err), "KubeRay-created Gateway should be deleted on suspend even when GatewayRef is set, got err=%v", err)
	})

	t.Run("same-named Gateway not controlled by the RayService is preserved", func(t *testing.T) {
		fakeClient, _, _, ownedGateway, _ := run(t, false)
		err := fakeClient.Get(ctx, types.NamespacedName{Name: ownedGateway.Name, Namespace: rsNamespace}, &gwv1.Gateway{})
		require.NoError(t, err, "a Gateway the RayService does not control must NOT be deleted on suspend")
	})
}

func TestCreateHTTPRoute(t *testing.T) {
	ctx := context.TODO()
	namespace := "test-ns"
	stepSize := int32(10)
	interval := int32(30)

	activeCluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "rayservice-active", Namespace: namespace}}
	pendingCluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "rayservice-pending", Namespace: namespace}}
	gateway := &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "test-rayservice-gateway", Namespace: namespace}}
	activeServeService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: utils.GenerateServeServiceName(activeCluster.Name), Namespace: namespace}}
	pendingServeService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: utils.GenerateServeServiceName(pendingCluster.Name), Namespace: namespace}}

	baseRayService := &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-rayservice", Namespace: namespace},
		Spec: rayv1.RayServiceSpec{
			UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
				Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
				ClusterUpgradeOptions: &rayv1.ClusterUpgradeOptions{
					StepSizePercent:  &stepSize,
					IntervalSeconds:  &interval,
					GatewayClassName: "istio",
				},
			},
		},
		Status: rayv1.RayServiceStatuses{
			ActiveServiceStatus: rayv1.RayServiceStatus{
				RayClusterName:       activeCluster.Name,
				TrafficRoutedPercent: new(int32(100)),
				TargetCapacity:       new(int32(100)),
			},
			PendingServiceStatus: rayv1.RayServiceStatus{
				RayClusterName:       pendingCluster.Name,
				TrafficRoutedPercent: new(int32(0)),
				TargetCapacity:       new(int32(30)),
			},
		},
	}

	tests := []struct {
		name                  string
		modifier              func(rs *rayv1.RayService)
		runtimeObjects        []runtime.Object
		expectError           bool
		expectedActiveWeight  int32
		expectedPendingWeight int32
		isPendingClusterReady bool
	}{
		{
			name: "NewClusterWithIncrementalUpgrade, but pending cluster is not ready, so no traffic shift.",
			modifier: func(rs *rayv1.RayService) {
				rs.Status.PendingServiceStatus.LastTrafficMigratedTime = &metav1.Time{Time: time.Now().Add(-time.Duration(interval+1) * time.Second)}
			},
			runtimeObjects:        []runtime.Object{activeCluster, pendingCluster, gateway, activeServeService, pendingServeService},
			isPendingClusterReady: false,
			expectedActiveWeight:  100,
			expectedPendingWeight: 0,
		},
		{
			name: "NewClusterWithIncrementalUpgrade, time since LastTrafficMigratedTime < IntervalSeconds.",
			modifier: func(rs *rayv1.RayService) {
				rs.Status.PendingServiceStatus.LastTrafficMigratedTime = &metav1.Time{Time: time.Now()}
			},
			runtimeObjects:        []runtime.Object{activeCluster, pendingCluster, gateway, activeServeService, pendingServeService},
			isPendingClusterReady: true,
			expectedActiveWeight:  100,
			expectedPendingWeight: 0,
		},
		{
			name: "NewClusterWithIncrementalUpgrade, time since LastTrafficMigratedTime >= IntervalSeconds.",
			modifier: func(rs *rayv1.RayService) {
				rs.Status.PendingServiceStatus.LastTrafficMigratedTime = &metav1.Time{Time: time.Now().Add(-time.Duration(interval+1) * time.Second)}
				rs.Status.PendingServiceStatus.TargetCapacity = new(int32(60))
			},
			runtimeObjects:        []runtime.Object{activeCluster, pendingCluster, gateway, activeServeService, pendingServeService},
			isPendingClusterReady: true,
			expectedActiveWeight:  90,
			expectedPendingWeight: 10,
		},
		{
			name: "NewClusterWithIncrementalUpgrade, TrafficRoutedPercent capped to pending TargetCapacity.",
			modifier: func(rs *rayv1.RayService) {
				rs.Status.PendingServiceStatus.LastTrafficMigratedTime = &metav1.Time{Time: time.Now().Add(-time.Duration(interval+1) * time.Second)}
				rs.Status.PendingServiceStatus.TargetCapacity = new(int32(5))
			},
			runtimeObjects:        []runtime.Object{activeCluster, pendingCluster, gateway, activeServeService, pendingServeService},
			isPendingClusterReady: true,
			expectedActiveWeight:  95,
			expectedPendingWeight: 5, // can only migrate 5% to pending until TargetCapacity reached
		},
		{
			name: "Create HTTPRoute called with missing ClusterUpgradeOptions.",
			modifier: func(rs *rayv1.RayService) {
				rs.Spec.UpgradeStrategy.ClusterUpgradeOptions = nil
			},
			runtimeObjects:        []runtime.Object{activeCluster, pendingCluster, gateway, activeServeService, pendingServeService},
			isPendingClusterReady: true,
			expectError:           true,
		},
		{
			name: "No on-going upgrade, pending cluster does not exist.",
			modifier: func(rs *rayv1.RayService) {
				rs.Status.PendingServiceStatus = rayv1.RayServiceStatus{}
			},
			runtimeObjects:        []runtime.Object{activeCluster, gateway, activeServeService},
			isPendingClusterReady: false,
			expectedActiveWeight:  100,
			expectedPendingWeight: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rayService := baseRayService.DeepCopy()
			tt.modifier(rayService)
			tt.runtimeObjects = append(tt.runtimeObjects, rayService)

			newScheme := runtime.NewScheme()
			_ = rayv1.AddToScheme(newScheme)
			_ = corev1.AddToScheme(newScheme)
			_ = gwv1.Install(newScheme)
			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(tt.runtimeObjects...).Build()

			reconciler := RayServiceReconciler{
				Client:   fakeClient,
				Scheme:   newScheme,
				Recorder: events.NewFakeRecorder(1),
			}

			route, err := reconciler.createHTTPRoute(ctx, rayService, tt.isPendingClusterReady)

			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, route)
			} else {
				require.NoError(t, err)
				require.NotNil(t, route)

				assert.Equal(t, "test-rayservice-httproute", route.Name)
				assert.Equal(t, "test-ns", route.Namespace)

				// Default (KubeRay-created) Gateway: ParentRef pins no listener, the
				// route matches all hosts and the "/" catch-all path (prior behavior).
				require.Len(t, route.Spec.ParentRefs, 1)
				assert.Nil(t, route.Spec.ParentRefs[0].SectionName)
				assert.Nil(t, route.Spec.ParentRefs[0].Port)
				assert.Empty(t, route.Spec.Hostnames)

				require.Len(t, route.Spec.Rules, 1)
				rule := route.Spec.Rules[0]
				require.Len(t, rule.Matches, 1)
				require.NotNil(t, rule.Matches[0].Path)
				require.NotNil(t, rule.Matches[0].Path.Value)
				assert.Equal(t, "/", *rule.Matches[0].Path.Value)

				require.GreaterOrEqual(t, len(rule.BackendRefs), 1)
				assert.Equal(t, gwv1.ObjectName(activeServeService.Name), rule.BackendRefs[0].BackendRef.Name)
				assert.Equal(t, tt.expectedActiveWeight, *rule.BackendRefs[0].Weight)

				if len(rule.BackendRefs) > 1 {
					assert.Equal(t, gwv1.ObjectName(pendingServeService.Name), rule.BackendRefs[1].BackendRef.Name)
					assert.Equal(t, tt.expectedPendingWeight, *rule.BackendRefs[1].Weight)
				} else {
					assert.Equal(t, int32(0), tt.expectedPendingWeight)
				}
			}
		})
	}
}

// When GatewayRef references a shared Gateway in another namespace, the
// HTTPRoute's ParentRef must target that Gateway, while the HTTPRoute itself and
// its backendRefs stay in the RayService's namespace (so owner-ref GC works and
// the Serve services resolve).
func TestCreateHTTPRouteWithGatewayRef(t *testing.T) {
	ctx := context.TODO()
	rsNamespace := "test-ns"
	gwNamespace := "gateways"
	stepSize := int32(10)
	interval := int32(30)

	activeCluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "rayservice-active", Namespace: rsNamespace}}
	// Shared Gateway lives in a different namespace from the RayService.
	sharedGateway := &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: "shared-gateway", Namespace: gwNamespace}}
	activeServeService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: utils.GenerateServeServiceName(activeCluster.Name), Namespace: rsNamespace}}

	rayService := &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-rayservice", Namespace: rsNamespace},
		Spec: rayv1.RayServiceSpec{
			UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
				Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
				ClusterUpgradeOptions: &rayv1.ClusterUpgradeOptions{
					StepSizePercent: &stepSize,
					IntervalSeconds: &interval,
					GatewayRef: &rayv1.GatewayReference{
						Name:        sharedGateway.Name,
						Namespace:   sharedGateway.Namespace,
						SectionName: "http",
						Port:        new(int32(80)),
					},
				},
			},
		},
		Status: rayv1.RayServiceStatuses{
			ActiveServiceStatus: rayv1.RayServiceStatus{
				RayClusterName:       activeCluster.Name,
				TrafficRoutedPercent: new(int32(100)),
				TargetCapacity:       new(int32(100)),
			},
		},
	}

	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)
	_ = gwv1.Install(newScheme)
	fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).
		WithRuntimeObjects(activeCluster, sharedGateway, activeServeService, rayService).Build()

	reconciler := RayServiceReconciler{
		Client:   fakeClient,
		Scheme:   newScheme,
		Recorder: events.NewFakeRecorder(1),
	}

	route, err := reconciler.createHTTPRoute(ctx, rayService, false)
	require.NoError(t, err)
	require.NotNil(t, route)

	// HTTPRoute lives in the RayService namespace.
	assert.Equal(t, "test-rayservice-httproute", route.Name)
	assert.Equal(t, rsNamespace, route.Namespace)

	// ParentRef targets the shared Gateway in its own namespace, pinned to the
	// requested listener (SectionName) and Port.
	require.Len(t, route.Spec.ParentRefs, 1)
	parentRef := route.Spec.ParentRefs[0]
	assert.Equal(t, gwv1.ObjectName(sharedGateway.Name), parentRef.Name)
	require.NotNil(t, parentRef.Namespace)
	assert.Equal(t, gwv1.Namespace(gwNamespace), *parentRef.Namespace)
	require.NotNil(t, parentRef.SectionName)
	assert.Equal(t, gwv1.SectionName("http"), *parentRef.SectionName)
	require.NotNil(t, parentRef.Port)
	assert.Equal(t, gwv1.PortNumber(80), *parentRef.Port)

	require.Len(t, route.Spec.Rules, 1)

	// Backend service references use the RayService namespace, not the Gateway's.
	require.GreaterOrEqual(t, len(route.Spec.Rules[0].BackendRefs), 1)
	backend := route.Spec.Rules[0].BackendRefs[0].BackendRef
	assert.Equal(t, gwv1.ObjectName(activeServeService.Name), backend.Name)
	require.NotNil(t, backend.Namespace)
	assert.Equal(t, gwv1.Namespace(rsNamespace), *backend.Namespace)
}

func TestReconcileHTTPRoute(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)
	_ = gwv1.Install(newScheme)

	ctx := context.TODO()
	namespace := "test-ns"
	stepSize := int32(10)
	interval := int32(30)
	gatewayName := "test-rayservice-gateway"
	routeName := "test-rayservice-httproute"

	activeCluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "active-ray-cluster", Namespace: namespace}}
	pendingCluster := &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "pending-ray-cluster", Namespace: namespace}}
	activeServeService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: utils.GenerateServeServiceName(activeCluster.Name), Namespace: namespace}}
	pendingServeService := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: utils.GenerateServeServiceName(pendingCluster.Name), Namespace: namespace}}
	gateway := &gwv1.Gateway{ObjectMeta: metav1.ObjectMeta{Name: gatewayName, Namespace: namespace}}

	baseRayService := &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-rayservice", Namespace: namespace},
		Spec: rayv1.RayServiceSpec{
			UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
				Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
				ClusterUpgradeOptions: &rayv1.ClusterUpgradeOptions{
					StepSizePercent:  &stepSize,
					IntervalSeconds:  &interval,
					GatewayClassName: "istio",
				},
			},
		},
		Status: rayv1.RayServiceStatuses{
			ActiveServiceStatus: rayv1.RayServiceStatus{
				RayClusterName:       activeCluster.Name,
				TrafficRoutedPercent: new(int32(100)),
				TargetCapacity:       new(int32(100)),
			},
			PendingServiceStatus: rayv1.RayServiceStatus{
				RayClusterName:       pendingCluster.Name,
				TrafficRoutedPercent: new(int32(0)),
				TargetCapacity:       new(int32(100)),
			},
		},
	}

	tests := []struct {
		modifier              func(rs *rayv1.RayService)
		existingRoute         *gwv1.HTTPRoute
		name                  string
		expectedActiveWeight  int32
		expectedPendingWeight int32
		pendingClusterExists  bool
		isPendingClusterReady bool
	}{
		{
			name:                  "Create HTTPRoute with no pending cluster.",
			isPendingClusterReady: false,
			pendingClusterExists:  false,
			expectedActiveWeight:  100,
			expectedPendingWeight: 0,
		},
		{
			name:                  "Create HTTPRoute when pending cluster exists, but is not ready.",
			isPendingClusterReady: false,
			pendingClusterExists:  true,
			expectedActiveWeight:  100,
			expectedPendingWeight: 0,
		},
		{
			name:                  "Create new HTTPRoute with existing weights.",
			isPendingClusterReady: true,
			pendingClusterExists:  true,
			expectedActiveWeight:  90,
			expectedPendingWeight: 10,
		},
		{
			name:                  "Update HTTPRoute when pending cluster is ready.",
			isPendingClusterReady: true,
			pendingClusterExists:  true,
			expectedActiveWeight:  90,
			expectedPendingWeight: 10,
		},
		{
			name:                  "Existing HTTPRoute, time since LastTrafficMigratedTime >= IntervalSeconds so updates HTTPRoute.",
			isPendingClusterReady: true,
			pendingClusterExists:  true,
			modifier: func(rs *rayv1.RayService) {
				rs.Status.PendingServiceStatus.LastTrafficMigratedTime = &metav1.Time{Time: time.Now().Add(-time.Duration(interval+1) * time.Second)}
			},
			existingRoute: &gwv1.HTTPRoute{
				ObjectMeta: metav1.ObjectMeta{Name: routeName, Namespace: namespace},
				Spec:       gwv1.HTTPRouteSpec{},
			},
			expectedActiveWeight:  90,
			expectedPendingWeight: 10,
		},
		{
			name:                  "Existing HTTPRoute, time since LastTrafficMigratedTime < IntervalSeconds so no update.",
			isPendingClusterReady: true,
			pendingClusterExists:  true,
			modifier: func(rs *rayv1.RayService) {
				rs.Status.PendingServiceStatus.LastTrafficMigratedTime = &metav1.Time{Time: time.Now()}
			},
			expectedActiveWeight:  100,
			expectedPendingWeight: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rayService := baseRayService.DeepCopy()
			if tt.modifier != nil {
				tt.modifier(rayService)
			}

			if !tt.pendingClusterExists {
				rayService.Status.PendingServiceStatus.RayClusterName = ""
			}

			runtimeObjects := []runtime.Object{rayService, activeCluster, pendingCluster, gateway, activeServeService, pendingServeService}
			if tt.existingRoute != nil {
				runtimeObjects = append(runtimeObjects, tt.existingRoute)
			}

			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(runtimeObjects...).Build()
			reconciler := RayServiceReconciler{Client: fakeClient, Scheme: newScheme, Recorder: events.NewFakeRecorder(10)}

			reconciledRoute, err := reconciler.reconcileHTTPRoute(ctx, rayService, tt.isPendingClusterReady)
			require.NoError(t, err)

			require.Len(t, reconciledRoute.Spec.Rules, 1)
			rule := reconciledRoute.Spec.Rules[0]
			if tt.pendingClusterExists {
				require.Len(t, rule.BackendRefs, 2)
				// Assert weights are set as expected.
				assert.Equal(t, tt.expectedActiveWeight, *rule.BackendRefs[0].Weight)
				assert.Equal(t, tt.expectedPendingWeight, *rule.BackendRefs[1].Weight)
			} else {
				require.Len(t, rule.BackendRefs, 1)
				// Assert active weight is as expected.
				assert.Equal(t, tt.expectedActiveWeight, *rule.BackendRefs[0].Weight)
			}
			// Assert ParentRef namespace is correctly set.
			parent := reconciledRoute.Spec.ParentRefs[0]
			assert.Equal(t, gwv1.ObjectName(gatewayName), parent.Name)
			assert.Equal(t, new(gwv1.Namespace(namespace)), parent.Namespace)
		})
	}
}

func TestReconcileGateway(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)
	_ = gwv1.Install(newScheme)

	ctx := context.TODO()
	namespace := "test-ns"

	rayService := makeIncrementalUpgradeRayService(
		true,
		"gateway-class",
		new(int32(20)),
		new(int32(30)),
		new(int32(80)),
		new(metav1.Now()),
	)
	gateway := makeGateway(fmt.Sprintf("%s-gateway", rayService.Name), rayService.Namespace, true)

	tests := []struct {
		name                 string
		expectedGatewayName  string
		expectedClass        string
		runtimeObjects       []runtime.Object
		expectedNumListeners int
	}{
		{
			name:                 "creates new Gateway if missing",
			runtimeObjects:       []runtime.Object{rayService},
			expectedGatewayName:  "incremental-ray-service-gateway",
			expectedClass:        "gateway-class",
			expectedNumListeners: 1,
		},
		{
			name:                 "updates Gateway if spec differs",
			runtimeObjects:       []runtime.Object{rayService, gateway},
			expectedGatewayName:  "incremental-ray-service-gateway",
			expectedClass:        "gateway-class",
			expectedNumListeners: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fakeClient := clientFake.NewClientBuilder().
				WithScheme(newScheme).
				WithRuntimeObjects(tt.runtimeObjects...).
				Build()

			reconciler := RayServiceReconciler{
				Client:   fakeClient,
				Scheme:   newScheme,
				Recorder: events.NewFakeRecorder(10),
			}

			err := reconciler.reconcileGateway(ctx, rayService)
			require.NoError(t, err)

			reconciledGateway := &gwv1.Gateway{}
			err = fakeClient.Get(ctx, client.ObjectKey{Name: tt.expectedGatewayName, Namespace: namespace}, reconciledGateway)
			require.NoError(t, err, "Failed to get the reconciled Gateway")

			assert.Equal(t, tt.expectedGatewayName, reconciledGateway.Name)
			assert.Equal(t, namespace, reconciledGateway.Namespace)
			assert.Equal(t, gwv1.ObjectName(tt.expectedClass), reconciledGateway.Spec.GatewayClassName)
			assert.Len(t, reconciledGateway.Spec.Listeners, tt.expectedNumListeners)
		})
	}
}

func TestReconcileServeTargetCapacity(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)

	tests := []struct {
		name                    string
		updatedCluster          string
		activeCapacity          int32
		pendingCapacity         int32
		activeRoutedPercent     int32
		pendingRoutedPercent    int32
		maxSurgePercent         int32
		expectedActiveCapacity  int32
		expectedPendingCapacity int32
	}{
		{
			name:                    "Scale up pending RayCluster when total TargetCapacity < 100",
			pendingRoutedPercent:    10,
			activeCapacity:          70,
			pendingCapacity:         10,
			maxSurgePercent:         20,
			expectedActiveCapacity:  70,
			expectedPendingCapacity: 30,
			updatedCluster:          "pending",
		},
		{
			name:                    "Scale down active RayCluster when total TargetCapacity > 100",
			pendingRoutedPercent:    30,
			activeCapacity:          80,
			pendingCapacity:         30,
			maxSurgePercent:         20,
			expectedActiveCapacity:  60,
			expectedPendingCapacity: 30,
			updatedCluster:          "active",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.TODO()
			rayService := &rayv1.RayService{
				Spec: rayv1.RayServiceSpec{
					UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
						Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
						ClusterUpgradeOptions: &rayv1.ClusterUpgradeOptions{
							MaxSurgePercent: new(tt.maxSurgePercent),
						},
					},
					ServeConfigV2: `{"target_capacity": 0}`,
				},
				Status: rayv1.RayServiceStatuses{
					ActiveServiceStatus: rayv1.RayServiceStatus{
						RayClusterName:       "active",
						TargetCapacity:       new(tt.activeCapacity),
						TrafficRoutedPercent: new(tt.activeRoutedPercent),
					},
					PendingServiceStatus: rayv1.RayServiceStatus{
						RayClusterName:       "pending",
						TargetCapacity:       new(tt.pendingCapacity),
						TrafficRoutedPercent: new(tt.pendingRoutedPercent),
					},
				},
			}

			var rayCluster *rayv1.RayCluster
			if tt.updatedCluster == "active" {
				rayCluster = &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "active"}}
			} else {
				rayCluster = &rayv1.RayCluster{ObjectMeta: metav1.ObjectMeta{Name: "pending"}}
			}

			fakeDashboard := &utils.FakeRayDashboardClient{}
			reconciler := &RayServiceReconciler{
				ServeConfigs: lru.New(10),
			}

			err := reconciler.reconcileServeTargetCapacity(ctx, rayService, rayCluster, fakeDashboard)
			require.NoError(t, err)
			require.NotEmpty(t, fakeDashboard.LastUpdatedConfig)

			if tt.updatedCluster == "active" {
				assert.Equal(t, tt.expectedActiveCapacity, *rayService.Status.ActiveServiceStatus.TargetCapacity)
				assert.Equal(t, tt.pendingCapacity, *rayService.Status.PendingServiceStatus.TargetCapacity)
				expectedServeConfig := `{"target_capacity":` + strconv.Itoa(int(tt.expectedActiveCapacity)) + `}`
				assert.JSONEq(t, expectedServeConfig, string(fakeDashboard.LastUpdatedConfig))
			} else {
				assert.Equal(t, tt.expectedPendingCapacity, *rayService.Status.PendingServiceStatus.TargetCapacity)
				assert.Equal(t, tt.activeCapacity, *rayService.Status.ActiveServiceStatus.TargetCapacity)
				expectedServeConfig := `{"target_capacity":` + strconv.Itoa(int(tt.expectedPendingCapacity)) + `}`
				assert.JSONEq(t, expectedServeConfig, string(fakeDashboard.LastUpdatedConfig))
			}
		})
	}
}

// MakeGateway is a helper function to return an Gateway object
func makeGateway(name, namespace string, isReady bool) *gwv1.Gateway {
	status := metav1.ConditionFalse
	if isReady {
		status = metav1.ConditionTrue
	}
	return &gwv1.Gateway{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: gwv1.GatewayStatus{
			Conditions: []metav1.Condition{
				{
					Type:   string(gwv1.GatewayConditionAccepted),
					Status: status,
				},
				{
					Type:   string(gwv1.GatewayConditionProgrammed),
					Status: status,
				},
			},
		},
	}
}

// MakeHTTPRoute is a helper function to return an HTTPRoute object
func makeHTTPRoute(name, namespace string, isReady bool) *gwv1.HTTPRoute {
	status := metav1.ConditionFalse
	if isReady {
		status = metav1.ConditionTrue
	}
	return &gwv1.HTTPRoute{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Status: gwv1.HTTPRouteStatus{
			RouteStatus: gwv1.RouteStatus{
				Parents: []gwv1.RouteParentStatus{
					{
						ParentRef: gwv1.ParentReference{
							Name:      gwv1.ObjectName("test-rayservice-gateway"),
							Namespace: new(gwv1.Namespace(namespace)),
						},
						Conditions: []metav1.Condition{
							{
								Type:   string(gwv1.RouteConditionAccepted),
								Status: status,
							},
							{
								Type:   string(gwv1.RouteConditionResolvedRefs),
								Status: status,
							},
						},
					},
				},
			},
		},
	}
}

func TestCheckIfNeedTargetCapacityUpdate(t *testing.T) {
	rayServiceName := "test-rayservice"
	gatewayName := fmt.Sprintf("%s-%s", rayServiceName, "gateway")
	httpRouteName := fmt.Sprintf("%s-%s", rayServiceName, "httproute")
	namespace := "test-ns"

	tests := []struct {
		name                string
		expectedReason      string
		runtimeObjects      []runtime.Object
		activeStatus        rayv1.RayServiceStatus
		pendingStatus       rayv1.RayServiceStatus
		expectedNeedsUpdate bool
	}{
		{
			name:                "Missing RayClusterNames",
			expectedNeedsUpdate: false,
			expectedReason:      "Both active and pending RayCluster instances are required for NewClusterWithIncrementalUpgrade.",
		},
		{
			name:          "Gateway not ready",
			activeStatus:  rayv1.RayServiceStatus{RayClusterName: "active"},
			pendingStatus: rayv1.RayServiceStatus{RayClusterName: "pending"},
			runtimeObjects: []runtime.Object{
				makeGateway(gatewayName, namespace, false), makeHTTPRoute(httpRouteName, namespace, true),
			},
			expectedNeedsUpdate: false,
			expectedReason:      "Gateway for RayService NewClusterWithIncrementalUpgrade is not ready.",
		},
		{
			name:          "HTTPRoute not ready",
			activeStatus:  rayv1.RayServiceStatus{RayClusterName: "active"},
			pendingStatus: rayv1.RayServiceStatus{RayClusterName: "pending"},
			runtimeObjects: []runtime.Object{
				makeGateway(gatewayName, namespace, true), makeHTTPRoute(httpRouteName, namespace, false),
			},
			expectedNeedsUpdate: false,
			expectedReason:      "HTTPRoute for RayService NewClusterWithIncrementalUpgrade is not ready.",
		},
		{
			name: "NewClusterWithIncrementalUpgrade is complete",
			activeStatus: rayv1.RayServiceStatus{
				RayClusterName:       "active",
				TargetCapacity:       new(int32(0)),
				TrafficRoutedPercent: new(int32(0)),
			},
			pendingStatus: rayv1.RayServiceStatus{
				RayClusterName:       "pending",
				TargetCapacity:       new(int32(100)),
				TrafficRoutedPercent: new(int32(100)),
			},
			runtimeObjects: []runtime.Object{
				makeGateway(gatewayName, namespace, true), makeHTTPRoute(httpRouteName, namespace, true),
			},
			expectedNeedsUpdate: false,
			expectedReason:      "All traffic has migrated to the upgraded cluster and NewClusterWithIncrementalUpgrade is complete.",
		},
		{
			name: "Pending RayCluster is still incrementally scaling",
			activeStatus: rayv1.RayServiceStatus{
				RayClusterName:       "active",
				TargetCapacity:       new(int32(70)),
				TrafficRoutedPercent: new(int32(70)),
			},
			pendingStatus: rayv1.RayServiceStatus{
				RayClusterName:       "pending",
				TargetCapacity:       new(int32(30)),
				TrafficRoutedPercent: new(int32(30)),
			},
			runtimeObjects: []runtime.Object{
				makeGateway(gatewayName, namespace, true), makeHTTPRoute(httpRouteName, namespace, true),
			},
			expectedNeedsUpdate: true,
			expectedReason:      "Pending RayCluster has not finished scaling up.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newScheme := runtime.NewScheme()
			_ = corev1.AddToScheme(newScheme)
			_ = gwv1.Install(newScheme)
			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(tt.runtimeObjects...).Build()
			// Initialize RayService reconciler.
			ctx := context.TODO()
			r := RayServiceReconciler{
				Client:   fakeClient,
				Recorder: &events.FakeRecorder{},
				Scheme:   scheme.Scheme,
			}
			rayService := &rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{Name: rayServiceName, Namespace: namespace},
				Status: rayv1.RayServiceStatuses{
					ActiveServiceStatus:  tt.activeStatus,
					PendingServiceStatus: tt.pendingStatus,
				},
			}
			needsUpdate, reason := r.checkIfNeedTargetCapacityUpdate(ctx, rayService)
			assert.Equal(t, tt.expectedNeedsUpdate, needsUpdate)
			assert.Equal(t, tt.expectedReason, reason)
		})
	}
}

func TestReconcilePerClusterServeService(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)

	ctx := context.TODO()
	namespace := "test-ns"

	// Minimal RayCluster with at least one container.
	rayCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-ray-cluster",
			Namespace: namespace,
			UID:       "test-uid",
		},
		Spec: rayv1.RayClusterSpec{
			HeadGroupSpec: rayv1.HeadGroupSpec{
				Template: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "ray-head"},
						},
					},
				},
			},
		},
	}
	rayService := makeIncrementalUpgradeRayService(
		true,
		"istio",
		new(int32(20)),
		new(int32(30)),
		new(int32(80)),
		new(metav1.Now()),
	)

	// The expected pending RayCluster serve service.
	expectedServeSvcName := utils.GenerateServeServiceName(rayCluster.Name)
	expectedServeService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      expectedServeSvcName,
			Namespace: namespace,
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				utils.RayClusterLabelKey:               rayCluster.Name,
				utils.RayClusterServingServiceLabelKey: "true",
			},
		},
	}

	tests := []struct {
		name                 string
		rayCluster           *rayv1.RayCluster
		runtimeObjects       []runtime.Object
		expectServiceCreated bool
		expectError          bool
	}{
		{
			name:                 "RayCluster is nil, no-op.",
			rayCluster:           nil,
			runtimeObjects:       []runtime.Object{rayService},
			expectServiceCreated: false,
			expectError:          false,
		},
		{
			name:                 "Create a new Serve service for the RayCluster.",
			rayCluster:           rayCluster,
			runtimeObjects:       []runtime.Object{rayService, rayCluster},
			expectServiceCreated: true,
			expectError:          false,
		},
		{
			name:                 "Pending RayCluster serve service already exists, no-op.",
			rayCluster:           rayCluster,
			runtimeObjects:       []runtime.Object{rayService, rayCluster, expectedServeService},
			expectServiceCreated: false,
			expectError:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newScheme := runtime.NewScheme()
			_ = rayv1.AddToScheme(newScheme)
			_ = corev1.AddToScheme(newScheme)

			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).WithRuntimeObjects(tt.runtimeObjects...).Build()
			reconciler := RayServiceReconciler{
				Client:   fakeClient,
				Scheme:   newScheme,
				Recorder: events.NewFakeRecorder(1),
			}

			err := reconciler.reconcilePerClusterServeService(ctx, rayService, tt.rayCluster)

			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			reconciledSvc := &corev1.Service{}
			err = fakeClient.Get(ctx, client.ObjectKey{Name: expectedServeSvcName, Namespace: namespace}, reconciledSvc)

			// No-op case, no service should be created when RayCluster is nil.
			if tt.rayCluster == nil {
				assert.True(t, errors.IsNotFound(err))
				return
			}

			// Otherwise, a valid serve service should be created for the RayCluster.
			require.NoError(t, err, "The Serve service should exist in the client")

			// Validate the expected Serve service exists for the RayCluster.
			require.NotNil(t, reconciledSvc)
			assert.Equal(t, expectedServeSvcName, reconciledSvc.Name)

			createdSvc := &corev1.Service{}
			err = fakeClient.Get(ctx, client.ObjectKey{Name: expectedServeSvcName, Namespace: namespace}, createdSvc)
			require.NoError(t, err, "The Serve service should exist in the client")

			// Verify the Serve service selector.
			expectedSelector := map[string]string{
				utils.RayClusterLabelKey:               rayCluster.Name,
				utils.RayClusterServingServiceLabelKey: "true",
			}
			assert.Equal(t, expectedSelector, createdSvc.Spec.Selector)

			// Validate owner ref is set to the expected RayCluster.
			if tt.expectServiceCreated {
				require.Len(t, createdSvc.OwnerReferences, 1)
				ownerRef := createdSvc.OwnerReferences[0]
				assert.Equal(t, rayCluster.Name, ownerRef.Name)
				assert.Equal(t, "RayCluster", ownerRef.Kind)
				assert.Equal(t, rayCluster.UID, ownerRef.UID)
			}
		})
	}
}

func TestIsInitializingTimeout(t *testing.T) {
	tests := []struct {
		name       string
		conditions []metav1.Condition
		expected   bool
	}{
		{
			name:       "No conditions",
			conditions: nil,
			expected:   false,
		},
		{
			name: "Failed due to timeout - any generation",
			conditions: []metav1.Condition{
				{
					Type:               string(rayv1.RayServiceReady),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.RayServiceInitializingTimeout),
					ObservedGeneration: 1,
				},
			},
			expected: true,
		},
		{
			name: "Failed due to timeout - different ObservedGeneration should still return true",
			conditions: []metav1.Condition{
				{
					Type:               string(rayv1.RayServiceReady),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.RayServiceInitializingTimeout),
					ObservedGeneration: 1,
				},
			},
			expected: true,
		},
		{
			name: "Failed for different reason",
			conditions: []metav1.Condition{
				{
					Type:               string(rayv1.RayServiceReady),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.ZeroServeEndpoints),
					ObservedGeneration: 5,
				},
			},
			expected: false,
		},
		{
			name: "Ready condition is True",
			conditions: []metav1.Condition{
				{
					Type:               string(rayv1.RayServiceReady),
					Status:             metav1.ConditionTrue,
					Reason:             string(rayv1.NonZeroServeEndpoints),
					ObservedGeneration: 5,
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &rayv1.RayService{
				Status: rayv1.RayServiceStatuses{
					Conditions: tt.conditions,
				},
			}

			result := isInitializingTimeout(rs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMarkFailedIfInitializingTimedOut(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)

	tests := []struct {
		annotations           map[string]string
		name                  string
		activeClusterName     string
		pendingClusterName    string
		conditions            []metav1.Condition
		generation            int64
		observedGeneration    int64
		timeInInitializing    time.Duration
		expectTimeout         bool
		expectClustersCleared bool
		expectEventRecorded   bool
	}{
		{
			name:        "No timeout annotation - should not timeout",
			annotations: nil,
			conditions: []metav1.Condition{
				{
					Type:               string(rayv1.RayServiceReady),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.RayServiceInitializing),
					LastTransitionTime: metav1.NewTime(time.Now().Add(-1 * time.Hour)),
					ObservedGeneration: 1,
				},
			},
			generation:          1,
			observedGeneration:  1,
			expectTimeout:       false,
			expectEventRecorded: false,
		},
		{
			name: "Timeout not exceeded - should not timeout",
			annotations: map[string]string{
				utils.RayServiceInitializingTimeoutAnnotation: "30m",
			},
			conditions: []metav1.Condition{
				{
					Type:               string(rayv1.RayServiceReady),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.RayServiceInitializing),
					LastTransitionTime: metav1.NewTime(time.Now().Add(-10 * time.Minute)),
					ObservedGeneration: 1,
				},
			},
			generation:          1,
			observedGeneration:  1,
			expectTimeout:       false,
			expectEventRecorded: false,
		},
		{
			name: "Timeout exceeded - should timeout and clear clusters",
			annotations: map[string]string{
				utils.RayServiceInitializingTimeoutAnnotation: "30m",
			},
			conditions: []metav1.Condition{
				{
					Type:               string(rayv1.RayServiceReady),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.RayServiceInitializing),
					LastTransitionTime: metav1.NewTime(time.Now().Add(-35 * time.Minute)),
					ObservedGeneration: 1,
				},
			},
			activeClusterName:     "active-cluster",
			pendingClusterName:    "pending-cluster",
			generation:            1,
			observedGeneration:    1,
			expectTimeout:         true,
			expectClustersCleared: true,
			expectEventRecorded:   true,
			timeInInitializing:    35 * time.Minute,
		},
		{
			name: "Already failed for current generation - should not timeout again",
			annotations: map[string]string{
				utils.RayServiceInitializingTimeoutAnnotation: "30m",
			},
			conditions: []metav1.Condition{
				{
					Type:               string(rayv1.RayServiceReady),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.RayServiceInitializingTimeout),
					LastTransitionTime: metav1.NewTime(time.Now().Add(-35 * time.Minute)),
					ObservedGeneration: 1,
				},
			},
			generation:          1,
			observedGeneration:  1,
			expectTimeout:       false,
			expectEventRecorded: false,
		},
		{
			name: "Not in Initializing state - should not timeout",
			annotations: map[string]string{
				utils.RayServiceInitializingTimeoutAnnotation: "30m",
			},
			conditions: []metav1.Condition{
				{
					Type:               string(rayv1.RayServiceReady),
					Status:             metav1.ConditionTrue,
					Reason:             string(rayv1.NonZeroServeEndpoints),
					LastTransitionTime: metav1.NewTime(time.Now().Add(-35 * time.Minute)),
					ObservedGeneration: 1,
				},
			},
			generation:          1,
			observedGeneration:  1,
			expectTimeout:       false,
			expectEventRecorded: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rs := &rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{
					Name:        "test-rayservice",
					Namespace:   "default",
					Generation:  tt.generation,
					Annotations: tt.annotations,
				},
				Status: rayv1.RayServiceStatuses{
					Conditions:         tt.conditions,
					ObservedGeneration: tt.observedGeneration,
					ActiveServiceStatus: rayv1.RayServiceStatus{
						RayClusterName: tt.activeClusterName,
					},
					PendingServiceStatus: rayv1.RayServiceStatus{
						RayClusterName: tt.pendingClusterName,
					},
				},
			}

			ctx := context.TODO()
			fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).Build()
			fakeRecorder := events.NewFakeRecorder(10)

			r := &RayServiceReconciler{
				Client:   fakeClient,
				Scheme:   scheme.Scheme,
				Recorder: fakeRecorder,
			}

			markFailedOnInitializingTimeout(ctx, r, rs)

			// Check if timeout occurred
			readyCond := meta.FindStatusCondition(rs.Status.Conditions, string(rayv1.RayServiceReady))
			require.NotNil(t, readyCond)

			if tt.expectTimeout {
				assert.Equal(t, metav1.ConditionFalse, readyCond.Status)
				assert.Equal(t, string(rayv1.RayServiceInitializingTimeout), readyCond.Reason)
				assert.Contains(t, readyCond.Message, "failed to become ready within the configured timeout")
			}

			// Check if clusters were cleared
			if tt.expectClustersCleared {
				assert.Empty(t, rs.Status.ActiveServiceStatus.RayClusterName)
				assert.Empty(t, rs.Status.PendingServiceStatus.RayClusterName)
			}

			// Check if event was recorded
			if tt.expectEventRecorded {
				select {
				case event := <-fakeRecorder.Events:
					assert.Contains(t, event, "RayService initializing timeout exceeded")
				case <-time.After(100 * time.Millisecond):
					t.Fatal("Expected event to be recorded but none was found")
				}
			} else {
				select {
				case <-fakeRecorder.Events:
					t.Fatal("Did not expect event to be recorded but one was found")
				case <-time.After(100 * time.Millisecond):
					// Expected - no event
				}
			}
		})
	}
}

// TestHandleSuspendResumeResetsReadyToInitializing verifies that resuming a
// RayService from the Suspended state resets the RayServiceReady condition
// back to Reason=Initializing with a fresh LastTransitionTime, so that
// markFailedOnInitializingTimeout can fire on the resumed attempt.
func TestHandleSuspendResumeResetsReadyToInitializing(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)

	// Stale timestamp from the suspend transition. If the resume path does
	// not refresh LastTransitionTime on RayServiceReady, the
	// initializing-timeout check will compare against this old time and
	// either fire instantly or never re-arm correctly.
	suspendTime := metav1.NewTime(time.Now().Add(-1 * time.Hour))

	rs := &rayv1.RayService{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-rayservice",
			Namespace: "default",
		},
		Spec: rayv1.RayServiceSpec{
			Suspend: false,
		},
		Status: rayv1.RayServiceStatuses{
			Conditions: []metav1.Condition{
				{
					Type:               string(rayv1.RayServiceSuspended),
					Status:             metav1.ConditionTrue,
					Reason:             string(rayv1.SuspendComplete),
					LastTransitionTime: suspendTime,
				},
				{
					Type:               string(rayv1.RayServiceSuspending),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.SuspendComplete),
					LastTransitionTime: suspendTime,
				},
				{
					Type:               string(rayv1.RayServiceReady),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.SuspendComplete),
					LastTransitionTime: suspendTime,
				},
				{
					Type:               string(rayv1.UpgradeInProgress),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.SuspendComplete),
					LastTransitionTime: suspendTime,
				},
				{
					Type:               string(rayv1.RollbackInProgress),
					Status:             metav1.ConditionFalse,
					Reason:             string(rayv1.SuspendComplete),
					LastTransitionTime: suspendTime,
				},
			},
		},
	}

	ctx := context.TODO()
	fakeClient := clientFake.NewClientBuilder().WithScheme(newScheme).Build()
	r := &RayServiceReconciler{
		Client:   fakeClient,
		Scheme:   scheme.Scheme,
		Recorder: events.NewFakeRecorder(10),
	}

	_, err := r.handleSuspend(ctx, rs)
	require.NoError(t, err)

	readyCond := meta.FindStatusCondition(rs.Status.Conditions, string(rayv1.RayServiceReady))
	require.NotNil(t, readyCond, "RayServiceReady condition must exist after resume")

	assert.Equal(t, string(rayv1.RayServiceInitializing), readyCond.Reason,
		"RayServiceReady reason must be Initializing on resume; otherwise initializing-timeout will not fire")

	assert.WithinDuration(t, time.Now(), readyCond.LastTransitionTime.Time, 5*time.Second,
		"RayServiceReady LastTransitionTime must reset on resume; otherwise initializing-timeout will use the stale suspend timestamp")

	upgradeCond := meta.FindStatusCondition(rs.Status.Conditions, string(rayv1.UpgradeInProgress))
	require.NotNil(t, upgradeCond, "UpgradeInProgress condition must exist after resume")

	assert.Equal(t, string(rayv1.RayServiceInitializing), upgradeCond.Reason,
		"UpgradeInProgress reason must be Initializing on resume; otherwise calculateConditions may set it to NoActiveCluster")

	assert.WithinDuration(t, time.Now(), upgradeCond.LastTransitionTime.Time, 5*time.Second,
		"UpgradeInProgress LastTransitionTime must reset on resume")
}

func Test_RayServiceReconcileManagedBy(t *testing.T) {
	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)
	_ = discoveryv1.AddToScheme(newScheme)

	const (
		// MultiKueueController represents the value of the MultiKueue controller
		MultiKueueController = "kueue.x-k8s.io/multikueue"
	)

	tests := []struct {
		managedBy       *string
		name            string
		shouldReconcile bool
	}{
		{
			managedBy:       nil,
			name:            "ManagedBy field not set",
			shouldReconcile: true,
		},
		{
			managedBy:       ptr.To(utils.KubeRayController),
			name:            "ManagedBy field to RayOperator",
			shouldReconcile: true,
		},
		{
			managedBy: new(""),
			name:      "ManagedBy field empty",
		},
		{
			managedBy: new(MultiKueueController),
			name:      "ManagedBy field to external allowed controller",
		},
		{
			managedBy: new("controller.com/invalid"),
			name:      "ManagedBy field to external not allowed controller",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			rayService := rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-service",
					Namespace: "default",
				},
				Spec: rayv1.RayServiceSpec{
					RayClusterSpec: rayv1.RayClusterSpec{
						HeadGroupSpec: rayv1.HeadGroupSpec{
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  "ray-head",
											Image: "rayproject/ray:latest",
										},
									},
								},
							},
						},
					},
				},
				Status: rayv1.RayServiceStatuses{},
			}
			rayService.Spec.ManagedBy = tc.managedBy

			runtimeObjects := []runtime.Object{&rayService}
			fakeClient := clientFake.NewClientBuilder().
				WithScheme(newScheme).
				WithRuntimeObjects(runtimeObjects...).
				WithStatusSubresource(&rayService).
				Build()

			testRayServiceReconciler := &RayServiceReconciler{
				Client:                       fakeClient,
				Recorder:                     &events.FakeRecorder{},
				Scheme:                       newScheme,
				ServeConfigs:                 lru.New(utils.ServeConfigLRUSize),
				RayClusterDeletionTimestamps: cmap.New[time.Time](),
			}

			request := ctrl.Request{
				NamespacedName: client.ObjectKey{
					Name:      rayService.Name,
					Namespace: rayService.Namespace,
				},
			}

			result, err := testRayServiceReconciler.Reconcile(ctx, request)
			require.NoError(t, err)

			if tc.shouldReconcile {
				// Should requeue for normal reconciliation
				assert.NotEqual(t, time.Duration(0), result.RequeueAfter)
			} else {
				// Should skip reconciliation (no requeue)
				assert.Equal(t, time.Duration(0), result.RequeueAfter)
			}
		})
	}
}

func TestReconcileRollbackState(t *testing.T) {
	ctx := context.TODO()
	namespace := "test-ns"

	baseSpec := rayv1.RayClusterSpec{
		RayVersion: "2.54.0",
		WorkerGroupSpecs: []rayv1.WorkerGroupSpec{
			{GroupName: "worker-group", Replicas: new(int32(1))},
		},
	}

	updatedSpec := baseSpec.DeepCopy()
	updatedSpec.RayVersion = "2.50.0"

	thirdSpec := baseSpec.DeepCopy()
	thirdSpec.RayVersion = "2.56.0"

	baseHash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(baseSpec)
	require.NoError(t, err)

	updatedHash, err := utils.GenerateHashWithoutReplicasAndWorkersToDelete(*updatedSpec)
	require.NoError(t, err)

	activeCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "active-cluster", Namespace: namespace, Annotations: map[string]string{utils.HashWithoutReplicasAndWorkersToDeleteKey: baseHash}},
	}
	pendingCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "pending-cluster", Namespace: namespace, Annotations: map[string]string{utils.HashWithoutReplicasAndWorkersToDeleteKey: updatedHash}},
	}

	tests := []struct {
		name                 string
		rayServiceSpec       rayv1.RayClusterSpec
		pendingHashOverride  string
		isRollbackInProgress bool
		expectRollbackStatus bool
	}{
		{
			name:                 "Normal RayService upgrade, goal matches pending",
			rayServiceSpec:       *updatedSpec,
			pendingHashOverride:  updatedHash,
			isRollbackInProgress: false,
			expectRollbackStatus: false,
		},
		{
			name:                 "RayService Spec changed, initiate rollback",
			rayServiceSpec:       baseSpec,
			pendingHashOverride:  updatedHash,
			isRollbackInProgress: false,
			expectRollbackStatus: true,
		},
		{
			name:                 "Rollback in progress, continues rolling back",
			rayServiceSpec:       baseSpec,
			pendingHashOverride:  updatedHash,
			isRollbackInProgress: true,
			expectRollbackStatus: true,
		},
		{
			name:                 "Rollback canceled, user updated spec back to pending",
			rayServiceSpec:       *updatedSpec,
			pendingHashOverride:  updatedHash,
			isRollbackInProgress: true,
			expectRollbackStatus: false,
		},
		{
			name:                 "Third-spec scenario: Goal updated to third spec mid-upgrade",
			rayServiceSpec:       *thirdSpec,
			pendingHashOverride:  updatedHash,
			isRollbackInProgress: false,
			expectRollbackStatus: true,
		},
		{
			name:                 "Fast rollback: Goal matches active, but pending already updated to match active",
			rayServiceSpec:       baseSpec,
			pendingHashOverride:  baseHash,
			isRollbackInProgress: false,
			expectRollbackStatus: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rayService := &rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rayservice", Namespace: namespace},
				Spec: rayv1.RayServiceSpec{
					RayClusterSpec: tt.rayServiceSpec,
				},
				Status: rayv1.RayServiceStatuses{
					Conditions: []metav1.Condition{},
				},
			}

			if tt.isRollbackInProgress {
				setCondition(rayService, rayv1.RollbackInProgress, metav1.ConditionTrue, rayv1.DesiredClusterSpecChanged, "rolling back")
			}

			testPendingCluster := pendingCluster.DeepCopy()
			testPendingCluster.Annotations[utils.HashWithoutReplicasAndWorkersToDeleteKey] = tt.pendingHashOverride

			reconciler := RayServiceReconciler{
				Recorder: events.NewFakeRecorder(1),
			}

			err := reconciler.reconcileRollbackState(ctx, rayService, activeCluster, testPendingCluster)
			require.NoError(t, err)

			isCurrentlyRollingBack := meta.IsStatusConditionTrue(rayService.Status.Conditions, string(rayv1.RollbackInProgress))
			assert.Equal(t, tt.expectRollbackStatus, isCurrentlyRollingBack)

			if tt.expectRollbackStatus && !tt.isRollbackInProgress {
				cond := meta.FindStatusCondition(rayService.Status.Conditions, string(rayv1.RollbackInProgress))
				require.NotNil(t, cond)
				assert.Equal(t, string(rayv1.DesiredClusterSpecChanged), cond.Reason)
			}
		})
	}
}

// headReadyCluster is a helper to construct a RayCluster with a specific HeadPodReady condition.
func headReadyCluster(ready bool) *rayv1.RayCluster {
	status := metav1.ConditionFalse
	if ready {
		status = metav1.ConditionTrue
	}
	return &rayv1.RayCluster{
		Status: rayv1.RayClusterStatus{
			Conditions: []metav1.Condition{
				{
					Type:   string(rayv1.HeadPodReady),
					Status: status,
				},
			},
		},
	}
}

func TestShouldCompleteIncrementalRollback(t *testing.T) {
	tests := []struct {
		name           string
		activeTC       int32
		activeTRP      int32
		pendingTC      int32
		pendingTRP     int32
		pendingCluster *rayv1.RayCluster
		want           bool
	}{
		{
			name:           "healthy pending at zero capacity and traffic — complete",
			activeTC:       100,
			activeTRP:      100,
			pendingTC:      0,
			pendingTRP:     0,
			pendingCluster: headReadyCluster(true),
			want:           true,
		},
		{
			name:           "unhealthy pending with leftover capacity — bypass complete",
			activeTC:       100,
			activeTRP:      100,
			pendingTC:      30,
			pendingTRP:     0,
			pendingCluster: headReadyCluster(false),
			want:           true,
		},
		{
			name:           "no pending RayCluster — complete when active is full",
			activeTC:       100,
			activeTRP:      100,
			pendingTC:      0,
			pendingTRP:     0,
			pendingCluster: nil,
			want:           true,
		},
		{
			name:           "healthy pending still holding target capacity — not complete",
			activeTC:       100,
			activeTRP:      100,
			pendingTC:      50,
			pendingTRP:     0,
			pendingCluster: headReadyCluster(true),
			want:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			activeStatus := &rayv1.RayServiceStatus{
				TargetCapacity:       new(tt.activeTC),
				TrafficRoutedPercent: new(tt.activeTRP),
			}
			pendingStatus := &rayv1.RayServiceStatus{
				TargetCapacity:       new(tt.pendingTC),
				TrafficRoutedPercent: new(tt.pendingTRP),
			}

			got := shouldCompleteIncrementalRollback(activeStatus, pendingStatus, tt.pendingCluster)
			if got != tt.want {
				t.Errorf("shouldCompleteIncrementalRollback() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReconcileServe_SkipConfigUpdateDuringRollback(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.RayClusterStatusConditions, true)
	features.SetFeatureGateDuringTest(t, features.RayServiceIncrementalUpgrade, true)
	ctx := context.TODO()
	namespace := "test-ns"

	activeCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "active-cluster", Namespace: namespace},
		Status: rayv1.RayClusterStatus{
			Conditions: []metav1.Condition{
				{Type: string(rayv1.HeadPodReady), Status: metav1.ConditionTrue},
			},
		},
	}

	pendingCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "pending-cluster", Namespace: namespace},
		Status: rayv1.RayClusterStatus{
			Conditions: []metav1.Condition{
				{Type: string(rayv1.HeadPodReady), Status: metav1.ConditionTrue},
			},
		},
	}

	activeServiceName, _ := utils.GenerateHeadServiceName(utils.RayClusterCRD, rayv1.RayClusterSpec{}, "active-cluster")
	activeService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: activeServiceName, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: utils.DashboardPortName, Port: 8265}},
		},
	}

	pendingServiceName, _ := utils.GenerateHeadServiceName(utils.RayClusterCRD, rayv1.RayClusterSpec{}, "pending-cluster")
	pendingService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: pendingServiceName, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Ports: []corev1.ServicePort{{Name: utils.DashboardPortName, Port: 8265}},
		},
	}

	tests := []struct {
		name            string
		clusterInstance *rayv1.RayCluster
		isRollback      bool
		isActiveCluster bool
		expectUpdate    bool
	}{
		{
			name:            "Active cluster during rollback -> should update",
			clusterInstance: activeCluster,
			isRollback:      true,
			isActiveCluster: true,
			expectUpdate:    true,
		},
		{
			name:            "Pending cluster during rollback -> should skip update",
			clusterInstance: pendingCluster,
			isRollback:      true,
			isActiveCluster: false,
			expectUpdate:    false,
		},
		{
			name:            "Pending cluster during upgrade (no rollback) -> should update",
			clusterInstance: pendingCluster,
			isRollback:      false,
			isActiveCluster: false,
			expectUpdate:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newScheme := runtime.NewScheme()
			_ = rayv1.AddToScheme(newScheme)
			_ = corev1.AddToScheme(newScheme)

			fakeClient := clientFake.NewClientBuilder().
				WithScheme(newScheme).
				WithRuntimeObjects(activeCluster, pendingCluster, activeService, pendingService).
				Build()

			fakeDashboardClient := &utils.FakeRayDashboardClient{}
			fakeDashboardClient.SetMultiApplicationStatuses(map[string]*utiltypes.ServeApplicationStatus{
				"app1": {Status: rayv1.ApplicationStatusEnum.RUNNING},
			})

			reconciler := &RayServiceReconciler{
				Client:   fakeClient,
				Recorder: &events.FakeRecorder{},
				dashboardClientFunc: func(_ *rayv1.RayCluster, _ string) (dashboardclient.RayDashboardClientInterface, error) {
					return fakeDashboardClient, nil
				},
				ServeConfigs: lru.New(10),
			}

			rayService := &rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rayservice", Namespace: namespace},
				Spec: rayv1.RayServiceSpec{
					UpgradeStrategy: &rayv1.RayServiceUpgradeStrategy{
						Type: ptr.To(rayv1.RayServiceNewClusterWithIncrementalUpgrade),
					},
					ServeConfigV2: `{"key": "new-config"}`,
				},
				Status: rayv1.RayServiceStatuses{
					ActiveServiceStatus: rayv1.RayServiceStatus{
						RayClusterName: "active-cluster",
					},
					PendingServiceStatus: rayv1.RayServiceStatus{
						RayClusterName: "pending-cluster",
					},
				},
			}

			if tt.isRollback {
				setCondition(rayService, rayv1.RollbackInProgress, metav1.ConditionTrue, rayv1.DesiredClusterSpecChanged, "rolling back")
				setCondition(rayService, rayv1.UpgradeInProgress, metav1.ConditionTrue, rayv1.DesiredClusterSpecChanged, "upgrade still in progress during rollback")
			} else {
				setCondition(rayService, rayv1.UpgradeInProgress, metav1.ConditionTrue, rayv1.DesiredClusterSpecChanged, "upgrading")
			}

			// Run reconcileServe
			_, _, err := reconciler.reconcileServe(ctx, rayService, tt.clusterInstance)
			require.NoError(t, err)

			// Check if UpdateDeployments was called
			if tt.expectUpdate {
				assert.NotNil(t, fakeDashboardClient.LastUpdatedConfig)
				assert.Contains(t, string(fakeDashboardClient.LastUpdatedConfig), "new-config")
			} else {
				assert.Nil(t, fakeDashboardClient.LastUpdatedConfig)
			}
		})
	}
}

func TestRayServiceFinalizer(t *testing.T) {
	ctx := context.TODO()
	namespace := "test-ns"

	newScheme := runtime.NewScheme()
	_ = rayv1.AddToScheme(newScheme)
	_ = corev1.AddToScheme(newScheme)
	_ = discoveryv1.AddToScheme(newScheme)

	tests := []struct {
		name            string
		rayService      *rayv1.RayService
		existingObjects []client.Object
		validate        func(t *testing.T, fakeClient client.Client, namespacedName types.NamespacedName)
	}{
		{
			name: "Add finalizer",
			rayService: &rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rayservice", Namespace: namespace},
			},
			validate: func(t *testing.T, fakeClient client.Client, namespacedName types.NamespacedName) {
				updatedRayService := &rayv1.RayService{}
				err := fakeClient.Get(ctx, namespacedName, updatedRayService)
				require.NoError(t, err)
				require.Len(t, updatedRayService.Finalizers, 1)
				assert.Equal(t, utils.RayServiceFinalizer, updatedRayService.Finalizers[0])
			},
		},
		{
			name: "Skip adding finalizer when managed by custom controller",
			rayService: &rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{Name: "test-rayservice", Namespace: namespace},
				Spec: rayv1.RayServiceSpec{
					ManagedBy: new("kueue.x-k8s.io/multikueue"),
				},
			},
			validate: func(t *testing.T, fakeClient client.Client, namespacedName types.NamespacedName) {
				updatedRayService := &rayv1.RayService{}
				err := fakeClient.Get(ctx, namespacedName, updatedRayService)
				require.NoError(t, err)
				assert.Empty(t, updatedRayService.Finalizers)
			},
		},
		{
			name: "Delete with clusters",
			rayService: &rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-rayservice",
					Namespace:         namespace,
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
					Finalizers:        []string{utils.RayServiceFinalizer},
				},
			},
			existingObjects: []client.Object{
				&rayv1.RayCluster{
					ObjectMeta: metav1.ObjectMeta{
						Name:      "test-cluster",
						Namespace: namespace,
						Labels: map[string]string{
							utils.RayOriginatedFromCRNameLabelKey: "test-rayservice",
							utils.RayOriginatedFromCRDLabelKey:    utils.RayOriginatedFromCRDLabelValue(utils.RayServiceCRD),
						},
					},
				},
			},
			validate: func(t *testing.T, fakeClient client.Client, namespacedName types.NamespacedName) {
				updatedRayService := &rayv1.RayService{}
				err := fakeClient.Get(ctx, namespacedName, updatedRayService)
				require.NoError(t, err)
				assert.Contains(t, updatedRayService.Finalizers, utils.RayServiceFinalizer)
			},
		},
		{
			name: "Delete without clusters",
			rayService: &rayv1.RayService{
				ObjectMeta: metav1.ObjectMeta{
					Name:              "test-rayservice",
					Namespace:         namespace,
					DeletionTimestamp: &metav1.Time{Time: time.Now()},
					Finalizers:        []string{utils.RayServiceFinalizer},
				},
			},
			validate: func(t *testing.T, fakeClient client.Client, namespacedName types.NamespacedName) {
				updatedRayService := &rayv1.RayService{}
				err := fakeClient.Get(ctx, namespacedName, updatedRayService)
				assert.True(t, errors.IsNotFound(err), "Expected NotFound error, got %v", err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs := []runtime.Object{tt.rayService}
			for _, obj := range tt.existingObjects {
				objs = append(objs, obj)
			}

			fakeClient := clientFake.NewClientBuilder().
				WithScheme(newScheme).
				WithRuntimeObjects(objs...).
				WithStatusSubresource(tt.rayService).
				Build()

			reconciler := &RayServiceReconciler{
				Client:                       fakeClient,
				Recorder:                     &events.FakeRecorder{},
				Scheme:                       newScheme,
				ServeConfigs:                 lru.New(10),
				RayClusterDeletionTimestamps: cmap.New[time.Time](),
			}

			namespacedName := types.NamespacedName{Name: tt.rayService.Name, Namespace: tt.rayService.Namespace}

			_, err := reconciler.Reconcile(ctx, ctrl.Request{NamespacedName: namespacedName})
			require.NoError(t, err)

			tt.validate(t, fakeClient, namespacedName)
		})
	}
}

func TestHandleSuspendObservedGeneration(t *testing.T) {
	ctx := context.Background()
	reconciler := &RayServiceReconciler{}

	t.Run("entering Suspending bumps observedGeneration on conditions", func(t *testing.T) {
		rayService := &rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "rayservice",
				Namespace:  "default",
				Generation: 5,
			},
			Spec: rayv1.RayServiceSpec{Suspend: true},
			Status: rayv1.RayServiceStatuses{
				ObservedGeneration: 2,
			},
		}

		_, err := reconciler.handleSuspend(ctx, rayService)
		require.NoError(t, err)
		assert.Equal(t, int64(5), rayService.Status.ObservedGeneration)

		suspending := meta.FindStatusCondition(rayService.Status.Conditions, string(rayv1.RayServiceSuspending))
		require.NotNil(t, suspending)
		assert.Equal(t, metav1.ConditionTrue, suspending.Status)
		assert.Equal(t, int64(5), suspending.ObservedGeneration)
	})

	t.Run("idle Suspended does not bump observedGeneration", func(t *testing.T) {
		rayService := &rayv1.RayService{
			ObjectMeta: metav1.ObjectMeta{
				Name:       "rayservice",
				Namespace:  "default",
				Generation: 9,
			},
			Spec: rayv1.RayServiceSpec{Suspend: true},
			Status: rayv1.RayServiceStatuses{
				ObservedGeneration: 4,
				Conditions: []metav1.Condition{
					{
						Type:   string(rayv1.RayServiceSuspended),
						Status: metav1.ConditionTrue,
						Reason: string(rayv1.SuspendComplete),
					},
				},
			},
		}

		_, err := reconciler.handleSuspend(ctx, rayService)
		require.NoError(t, err)
		assert.Equal(t, int64(4), rayService.Status.ObservedGeneration)
	})
}
