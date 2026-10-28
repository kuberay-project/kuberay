package yunikorn

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

func TestIsGangSchedulingEnabled(t *testing.T) {
	yk := &YuniKornScheduler{}
	// Test RayCluster
	appID := "job-1-01234"
	queue := "root.default"
	rayCluster1 := createRayClusterWithLabels(
		"ray-cluster-with-gang-scheduling",
		"test1",
		map[string]string{
			RayApplicationIDLabelName:      appID,
			RayApplicationQueueLabelName:   queue,
			utils.RayGangSchedulingEnabled: "true",
		},
	)

	assert.True(t, yk.isGangSchedulingEnabled(rayCluster1))

	rayCluster2 := createRayClusterWithLabels(
		"ray-cluster-with-gang-scheduling",
		"test2",
		map[string]string{
			RayApplicationIDLabelName:      appID,
			RayApplicationQueueLabelName:   queue,
			utils.RayGangSchedulingEnabled: "",
		},
	)

	assert.True(t, yk.isGangSchedulingEnabled(rayCluster2))

	rayCluster3 := createRayClusterWithLabels(
		"ray-cluster-with-gang-scheduling",
		"test3",
		map[string]string{
			RayApplicationIDLabelName:    appID,
			RayApplicationQueueLabelName: queue,
		},
	)

	assert.False(t, yk.isGangSchedulingEnabled(rayCluster3))

	// Test RayJob
	rayJob1 := createRayJobWithLabels(
		"ray-job-with-gang-scheduling",
		"test1",
		nil,
		map[string]string{
			RayApplicationIDLabelName:      appID,
			RayApplicationQueueLabelName:   queue,
			utils.RayGangSchedulingEnabled: "true",
		},
	)

	assert.True(t, yk.isGangSchedulingEnabled(rayJob1))

	rayJob2 := createRayJobWithLabels(
		"ray-job-with-gang-scheduling",
		"test2",
		nil,
		map[string]string{
			RayApplicationIDLabelName:      appID,
			RayApplicationQueueLabelName:   queue,
			utils.RayGangSchedulingEnabled: "",
		},
	)

	assert.True(t, yk.isGangSchedulingEnabled(rayJob2))

	rayJob3 := createRayJobWithLabels(
		"ray-job-with-gang-scheduling",
		"test3",
		nil,
		map[string]string{
			RayApplicationIDLabelName:    appID,
			RayApplicationQueueLabelName: queue,
		},
	)

	assert.False(t, yk.isGangSchedulingEnabled(rayJob3))
}

func TestPopulatePodLabelsFromRayCluster(t *testing.T) {
	testCases := []struct {
		name                   string
		job                    string
		queue                  string
		clusterName            string
		clusterNameSpace       string
		clusterLabel           map[string]string
		podName                string
		expectJobLabelResult   bool
		expectQueueLabelResult bool
	}{
		{
			name:             "Ray Cluster CR has labels defined",
			job:              "job-1-01234",
			queue:            "root.default",
			clusterName:      "ray-cluster-with-labels",
			clusterNameSpace: "test",
			clusterLabel: map[string]string{
				RayApplicationIDLabelName:    "job-1-01234",
				RayApplicationQueueLabelName: "root.default",
			},
			podName:                "my-pod-1",
			expectJobLabelResult:   true,
			expectQueueLabelResult: true,
		},
		{
			name:                   "Ray Cluster CR has nothing. In this case, the pod will not be populated with the required labels",
			job:                    "job-2-01234",
			queue:                  "root.default",
			clusterName:            "ray-cluster-with-labels",
			clusterNameSpace:       "test1",
			clusterLabel:           nil,
			podName:                "my-pod-2",
			expectJobLabelResult:   false,
			expectQueueLabelResult: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rayCluster := createRayClusterWithLabels(testCase.clusterName, testCase.clusterNameSpace, testCase.clusterLabel)
			rayPod := createPod(testCase.podName, testCase.clusterNameSpace)
			populateLabelsFromObject(rayCluster, rayPod, RayApplicationIDLabelName, YuniKornPodApplicationIDLabelName)
			populateLabelsFromObject(rayCluster, rayPod, RayApplicationQueueLabelName, YuniKornPodQueueLabelName)
			assert.Equal(t, podLabelsContains(rayPod, YuniKornPodApplicationIDLabelName, testCase.job), testCase.expectJobLabelResult)
			assert.Equal(t, podLabelsContains(rayPod, YuniKornPodQueueLabelName, testCase.queue), testCase.expectQueueLabelResult)
		})
	}
}

func TestPopulateRayClusterLabelsFromRayJob(t *testing.T) {
	testCases := []struct {
		RayJobLabel            map[string]string
		name                   string
		RayJobName             string
		RayJobNamespace        string
		expectJobLabelResult   bool
		expectQueueLabelResult bool
	}{
		{
			name:            "Ray Job CR has labels defined",
			RayJobName:      "ray-job-1",
			RayJobNamespace: "test",
			RayJobLabel: map[string]string{
				RayApplicationIDLabelName:    "job-1-01234",
				RayApplicationQueueLabelName: "root.default",
			},
			expectJobLabelResult:   true,
			expectQueueLabelResult: true,
		},
		{
			name:                   "Ray Job CR has nothing. In this case, the pod will not be populated with the required labels",
			RayJobName:             "ray-job-2",
			RayJobNamespace:        "test1",
			RayJobLabel:            nil,
			expectJobLabelResult:   false,
			expectQueueLabelResult: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rayJob := createRayJobWithLabels(testCase.RayJobName, testCase.RayJobNamespace, nil, testCase.RayJobLabel)
			rayCluster := createRayClusterWithLabels(testCase.name, testCase.RayJobNamespace, map[string]string{})
			populateLabelsFromObject(rayJob, rayCluster, RayApplicationIDLabelName, RayApplicationIDLabelName)
			populateLabelsFromObject(rayJob, rayCluster, RayApplicationQueueLabelName, RayApplicationQueueLabelName)
			assert.Equal(t, rayCluster.Labels[RayApplicationIDLabelName], testCase.RayJobLabel[RayApplicationIDLabelName])
			assert.Equal(t, rayCluster.Labels[RayApplicationQueueLabelName], testCase.RayJobLabel[RayApplicationQueueLabelName])
		})
	}
}

func TestPopulateLabelsFromRayJob(t *testing.T) {
	testCases := []struct {
		RayJobLabel            map[string]string
		name                   string
		RayJobName             string
		RayJobNamespace        string
		expectJobLabelResult   bool
		expectQueueLabelResult bool
	}{
		{
			name:            "Ray Job CR has labels defined",
			RayJobName:      "ray-job-1",
			RayJobNamespace: "test",
			RayJobLabel: map[string]string{
				RayApplicationIDLabelName:    "job-1-01234",
				RayApplicationQueueLabelName: "root.default",
			},
			expectJobLabelResult:   true,
			expectQueueLabelResult: true,
		},
		{
			name:                   "Ray Job CR has nothing. In this case, the pod will not be populated with the required labels",
			RayJobName:             "ray-job-2",
			RayJobNamespace:        "test1",
			RayJobLabel:            nil,
			expectJobLabelResult:   false,
			expectQueueLabelResult: false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			rayJob := createRayJobWithLabels(testCase.RayJobName, testCase.RayJobNamespace, nil, testCase.RayJobLabel)
			submitterPodTemplate := createSubmitterPodTemplate()
			populateLabelsFromObject(rayJob, submitterPodTemplate, RayApplicationIDLabelName, RayApplicationIDLabelName)
			populateLabelsFromObject(rayJob, submitterPodTemplate, RayApplicationQueueLabelName, RayApplicationQueueLabelName)
			assert.Equal(t, testCase.RayJobLabel[RayApplicationIDLabelName], submitterPodTemplate.Labels[RayApplicationIDLabelName])
			assert.Equal(t, testCase.RayJobLabel[RayApplicationQueueLabelName], submitterPodTemplate.Labels[RayApplicationQueueLabelName])
		})
	}
}

func TestPropagateTaskGroupsAnnotationToPod(t *testing.T) {
	appID := "job-1-01234"
	queue := "root.default"

	// test the case when gang-scheduling is enabled
	rayClusterWithGangScheduling := createRayClusterWithLabels(
		"ray-cluster-with-gang-scheduling",
		"test-namespace",
		map[string]string{
			RayApplicationIDLabelName:      appID,
			RayApplicationQueueLabelName:   queue,
			utils.RayGangSchedulingEnabled: "true",
		},
	)

	// head pod:
	//   cpu: 5
	//   memory: 5Gi
	addHeadPodSpec(rayClusterWithGangScheduling, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("5"),
		corev1.ResourceMemory: resource.MustParse("5Gi"),
	})

	// worker pod:
	//   cpu: 2
	//   memory: 10Gi
	//   nvidia.com/gpu: 1
	addWorkerPodSpec(rayClusterWithGangScheduling,
		"worker-group-1", 2, 2, 2, corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2"),
			corev1.ResourceMemory: resource.MustParse("10Gi"),
			"nvidia.com/gpu":      resource.MustParse("1"),
		})
	rayClusterWithGangScheduling.Spec.WorkerGroupSpecs[0].NumOfHosts = 3

	// gang-scheduling enabled case, the plugin should populate the taskGroup annotation to the app
	rayPod := createPod("ray-pod", "default")
	err := propagateTaskGroupsAnnotation(rayClusterWithGangScheduling, rayPod)
	require.NoError(t, err)

	kk, err := getTaskGroupsFromAnnotation(rayPod)
	require.NoError(t, err)
	assert.Len(t, kk, 2) // 1 head group, 1 worker group
	// verify the annotation value
	taskGroupsSpec := rayPod.Annotations[YuniKornTaskGroupsAnnotationName]
	assert.NotEmpty(t, taskGroupsSpec)
	taskGroups := newTaskGroups()
	err = taskGroups.unmarshalFrom(taskGroupsSpec)
	require.NoError(t, err)
	assert.Len(t, taskGroups.Groups, 2)

	// verify the correctness of head group
	headGroup := taskGroups.getTaskGroup(utils.RayNodeHeadGroupLabelValue)
	assert.NotNil(t, headGroup)
	assert.Equal(t, int32(1), headGroup.MinMember)
	assert.Equal(t, resource.MustParse("5"), headGroup.MinResource[corev1.ResourceCPU.String()])
	assert.Equal(t, resource.MustParse("5Gi"), headGroup.MinResource[corev1.ResourceMemory.String()])

	// verify the correctness of worker group
	workerGroup := taskGroups.getTaskGroup("worker-group-1")
	assert.NotNil(t, workerGroup)
	assert.Equal(t, int32(6), workerGroup.MinMember)
	assert.Equal(t, resource.MustParse("2"), workerGroup.MinResource[corev1.ResourceCPU.String()])
	assert.Equal(t, resource.MustParse("10Gi"), workerGroup.MinResource[corev1.ResourceMemory.String()])
	assert.Equal(t, resource.MustParse("1"), workerGroup.MinResource["nvidia.com/gpu"])
}

func TestPropagateTaskGroupsAnnotationToRayClusterAndSubmitterPodTemplate(t *testing.T) {
	appID := "job-1-01234"
	queue := "root.default"

	rayCluster := createRayClusterWithLabels(
		"ray-job-with-gang-scheduling",
		"test-namespace",
		map[string]string{},
	)

	addHeadPodSpec(rayCluster, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("5"),
		corev1.ResourceMemory: resource.MustParse("5Gi"),
	})

	addWorkerPodSpec(rayCluster, "worker-group-1", 1, 1, 1, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("2"),
		corev1.ResourceMemory: resource.MustParse("10Gi"),
	})

	rayJobWithGangScheduling := createRayJobWithLabels(
		"ray-job-with-gang-scheduling",
		"test-namespace",
		rayCluster.Spec.DeepCopy(),
		map[string]string{
			RayApplicationIDLabelName:      appID,
			RayApplicationQueueLabelName:   queue,
			utils.RayGangSchedulingEnabled: "true",
		},
	)

	submitterPodTemplate := createSubmitterPodTemplate()

	err := propagateTaskGroupsAnnotation(rayJobWithGangScheduling, rayCluster)
	require.NoError(t, err)

	// verify the correctness of rayCluster
	kk, err := getTaskGroupsFromRayCluster(rayCluster)
	require.NoError(t, err)
	assert.Len(t, kk, 3) // 1 head group, 1 worker group, 1 submitter group
	// verify the annotation value
	taskGroupsSpec := rayCluster.Annotations[YuniKornTaskGroupsAnnotationName]
	assert.NotEmpty(t, taskGroupsSpec)
	taskGroups := newTaskGroups()
	err = taskGroups.unmarshalFrom(taskGroupsSpec)
	require.NoError(t, err)
	assert.Len(t, taskGroups.Groups, 3)

	headGroup := taskGroups.getTaskGroup(utils.RayNodeHeadGroupLabelValue)
	assert.NotNil(t, headGroup)
	assert.Equal(t, int32(1), headGroup.MinMember)
	assert.Equal(t, resource.MustParse("5"), headGroup.MinResource[corev1.ResourceCPU.String()])
	assert.Equal(t, resource.MustParse("5Gi"), headGroup.MinResource[corev1.ResourceMemory.String()])

	workerGroup := taskGroups.getTaskGroup("worker-group-1")
	assert.NotNil(t, workerGroup)
	assert.Equal(t, int32(1), workerGroup.MinMember)
	assert.Equal(t, resource.MustParse("2"), workerGroup.MinResource[corev1.ResourceCPU.String()])
	assert.Equal(t, resource.MustParse("10Gi"), workerGroup.MinResource[corev1.ResourceMemory.String()])

	submitterGroup := taskGroups.getTaskGroup(utils.RayNodeSubmitterGroupLabelValue)
	assert.NotNil(t, submitterGroup)
	assert.Equal(t, int32(1), submitterGroup.MinMember)
	assert.Equal(t, resource.MustParse("500m"), submitterGroup.MinResource[corev1.ResourceCPU.String()])
	assert.Equal(t, resource.MustParse("200Mi"), submitterGroup.MinResource[corev1.ResourceMemory.String()])

	err = propagateTaskGroupsAnnotation(rayJobWithGangScheduling, submitterPodTemplate)
	require.NoError(t, err)
	// verify the correctness of k8s job
	kk, err = getTaskGroupsFromSubmitterPodTemplate(submitterPodTemplate)
	require.NoError(t, err)
	assert.Len(t, kk, 3) // 1 head group, 1 worker group, 1 submitter group
	// verify the annotation value
	taskGroupsSpec = submitterPodTemplate.Annotations[YuniKornTaskGroupsAnnotationName]
	assert.NotEmpty(t, taskGroupsSpec)
	taskGroups = newTaskGroups()
	err = taskGroups.unmarshalFrom(taskGroupsSpec)
	require.NoError(t, err)
	assert.Len(t, taskGroups.Groups, 3)

	headGroup = taskGroups.getTaskGroup(utils.RayNodeHeadGroupLabelValue)
	assert.NotNil(t, headGroup)
	assert.Equal(t, int32(1), headGroup.MinMember)
	assert.Equal(t, resource.MustParse("5"), headGroup.MinResource[corev1.ResourceCPU.String()])
	assert.Equal(t, resource.MustParse("5Gi"), headGroup.MinResource[corev1.ResourceMemory.String()])

	workerGroup = taskGroups.getTaskGroup("worker-group-1")
	assert.NotNil(t, workerGroup)
	assert.Equal(t, int32(1), workerGroup.MinMember)
	assert.Equal(t, resource.MustParse("2"), workerGroup.MinResource[corev1.ResourceCPU.String()])
	assert.Equal(t, resource.MustParse("10Gi"), workerGroup.MinResource[corev1.ResourceMemory.String()])

	submitterGroup = taskGroups.getTaskGroup(utils.RayNodeSubmitterGroupLabelValue)
	assert.NotNil(t, submitterGroup)
	assert.Equal(t, int32(1), submitterGroup.MinMember)
	assert.Equal(t, resource.MustParse("500m"), submitterGroup.MinResource[corev1.ResourceCPU.String()])
	assert.Equal(t, resource.MustParse("200Mi"), submitterGroup.MinResource[corev1.ResourceMemory.String()])
}

func TestAddMetadataToChildResourceFromRayCluster(t *testing.T) {
	yk := &YuniKornScheduler{}
	ctx := context.Background()

	rayCluster := createRayClusterWithLabels(
		"ray-cluster-without-gang-scheduling",
		"test-namespace",
		map[string]string{
			RayApplicationIDLabelName:    "job-1",
			RayApplicationQueueLabelName: "root.default",
		},
	)

	rayPod := createPod("ray-pod", "default")
	yk.AddMetadataToChildResource(ctx, rayCluster, rayPod, "ray-cluster-without-gang-scheduling")

	assert.Equal(t, "job-1", rayPod.Labels[YuniKornPodApplicationIDLabelName])
	assert.Equal(t, "root.default", rayPod.Labels[YuniKornPodQueueLabelName])
	assert.Equal(t, "yunikorn", rayPod.Spec.SchedulerName)

	rayCluster = createRayClusterWithLabels(
		"ray-cluster-with-gang-scheduling",
		"test-namespace",
		map[string]string{
			RayApplicationIDLabelName:      "job-2",
			RayApplicationQueueLabelName:   "root.default",
			utils.RayGangSchedulingEnabled: "true",
		},
	)
	addHeadPodSpec(rayCluster, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("1Gi"),
	})
	addWorkerPodSpec(rayCluster, "worker-group-1", 1, 1, 1, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("1Gi"),
	})

	rayPod = createPod("ray-pod", "default")
	yk.AddMetadataToChildResource(ctx, rayCluster, rayPod, "ray-cluster-with-gang-scheduling")

	assert.Equal(t, "job-2", rayPod.Labels[YuniKornPodApplicationIDLabelName])
	assert.Equal(t, "root.default", rayPod.Labels[YuniKornPodQueueLabelName])
	assert.Equal(t, "ray-cluster-with-gang-scheduling", rayPod.Annotations[YuniKornTaskGroupNameAnnotationName])
	assert.JSONEq(t, "[{\"minResource\":{\"cpu\":\"1\",\"memory\":\"1Gi\"},\"name\":\"headgroup\",\"minMember\":1},{\"minResource\":{\"cpu\":\"1\",\"memory\":\"1Gi\"},\"name\":\"worker-group-1\",\"minMember\":1}]", rayPod.Annotations[YuniKornTaskGroupsAnnotationName])
	assert.Equal(t, "yunikorn", rayPod.Spec.SchedulerName)
}

func TestAddMetadataToChildResourceFromRayJob(t *testing.T) {
	yk := &YuniKornScheduler{}
	ctx := context.Background()

	rayCluster := createRayClusterWithLabels(
		"ray-cluster-without-gang-scheduling",
		"test-namespace",
		map[string]string{},
	)
	rayJob := createRayJobWithLabels(
		"ray-job-without-gang-scheduling",
		"test-namespace",
		nil,
		map[string]string{
			RayApplicationIDLabelName:    "job-3",
			RayApplicationQueueLabelName: "root.default",
		},
	)

	submitterPodTemplate := createSubmitterPodTemplate()

	yk.AddMetadataToChildResource(ctx, rayJob, rayCluster, "")
	assert.Equal(t, "job-3", rayCluster.Labels[YuniKornPodApplicationIDLabelName])
	assert.Equal(t, "root.default", rayCluster.Labels[YuniKornPodQueueLabelName])
	assert.Empty(t, rayCluster.Annotations[YuniKornTaskGroupsAnnotationName]) // no task groups annotation since gang scheduling is not enabled
	yk.AddMetadataToChildResource(ctx, rayJob, submitterPodTemplate, "")
	assert.Equal(t, "job-3", submitterPodTemplate.Labels[YuniKornPodApplicationIDLabelName])
	assert.Equal(t, "root.default", submitterPodTemplate.Labels[YuniKornPodQueueLabelName])
	assert.Empty(t, submitterPodTemplate.Annotations[YuniKornTaskGroupsAnnotationName]) // no task groups annotation since gang scheduling is not enabled
	assert.Equal(t, "yunikorn", submitterPodTemplate.Spec.SchedulerName)

	rayCluster = createRayClusterWithLabels(
		"ray-cluster-with-gang-scheduling",
		"test-namespace",
		map[string]string{},
	)

	addHeadPodSpec(rayCluster, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("1Gi"),
	})
	addWorkerPodSpec(rayCluster, "worker-group", 1, 1, 1, corev1.ResourceList{
		corev1.ResourceCPU:    resource.MustParse("1"),
		corev1.ResourceMemory: resource.MustParse("1Gi"),
	})

	rayJob = createRayJobWithLabels(
		"ray-job-with-gang-scheduling",
		"test-namespace",
		&rayCluster.Spec,
		map[string]string{
			RayApplicationIDLabelName:      "job-4",
			RayApplicationQueueLabelName:   "root.default",
			utils.RayGangSchedulingEnabled: "true",
		},
	)
	submitterPodTemplate = createSubmitterPodTemplate()

	yk.AddMetadataToChildResource(ctx, rayJob, rayCluster, "")

	assert.Equal(t, "job-4", rayCluster.Labels[YuniKornPodApplicationIDLabelName])
	assert.Equal(t, "root.default", rayCluster.Labels[YuniKornPodQueueLabelName])
	assert.JSONEq(t, `[{"minResource":{"cpu":"1","memory":"1Gi"},"name":"headgroup","minMember":1},{"minResource":{"cpu":"1","memory":"1Gi"},"name":"worker-group","minMember":1},{"minResource":{"cpu":"500m","memory":"200Mi"},"name":"submittergroup","minMember":1}]`, rayCluster.Annotations[YuniKornTaskGroupsAnnotationName])

	yk.AddMetadataToChildResource(ctx, rayJob, submitterPodTemplate, utils.RayNodeSubmitterGroupLabelValue)
	assert.Equal(t, utils.RayNodeSubmitterGroupLabelValue, submitterPodTemplate.Annotations[YuniKornTaskGroupNameAnnotationName])
	assert.Equal(t, "job-4", submitterPodTemplate.Labels[YuniKornPodApplicationIDLabelName])
	assert.Equal(t, "root.default", submitterPodTemplate.Labels[YuniKornPodQueueLabelName])
	assert.Equal(t, "yunikorn", submitterPodTemplate.Spec.SchedulerName)
	assert.JSONEq(t, `[{"minResource":{"cpu":"1","memory":"1Gi"},"name":"headgroup","minMember":1},{"minResource":{"cpu":"1","memory":"1Gi"},"name":"worker-group","minMember":1},{"minResource":{"cpu":"500m","memory":"200Mi"},"name":"submittergroup","minMember":1}]`, submitterPodTemplate.Annotations[YuniKornTaskGroupsAnnotationName])
}

func createRayClusterWithLabels(name string, namespace string, labels map[string]string) *rayv1.RayCluster {
	rayCluster := &rayv1.RayCluster{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
	}

	return rayCluster
}

func createRayJobWithLabels(name string, namespace string, rayClusterSpec *rayv1.RayClusterSpec, labels map[string]string) *rayv1.RayJob {
	rayJob := &rayv1.RayJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    labels,
		},
		Spec: rayv1.RayJobSpec{
			RayClusterSpec: rayClusterSpec,
		},
	}

	return rayJob
}

func addHeadPodSpec(rayCluster *rayv1.RayCluster, resource corev1.ResourceList) {
	headContainers := []corev1.Container{
		{
			Name:  "head-pod",
			Image: "ray.io/ray-head:latest",
			Resources: corev1.ResourceRequirements{
				Limits:   nil,
				Requests: resource,
			},
		},
	}

	rayCluster.Spec.HeadGroupSpec.Template.Spec.Containers = headContainers
}

func addWorkerPodSpec(app *rayv1.RayCluster, workerGroupName string,
	replicas int32, minReplicas int32, maxReplicas int32, resources corev1.ResourceList,
) {
	workerContainers := []corev1.Container{
		{
			Name:  "worker-pod",
			Image: "ray.io/ray-head:latest",
			Resources: corev1.ResourceRequirements{
				Limits:   nil,
				Requests: resources,
			},
		},
	}

	app.Spec.WorkerGroupSpecs = append(app.Spec.WorkerGroupSpecs, rayv1.WorkerGroupSpec{
		GroupName:   workerGroupName,
		Replicas:    &replicas,
		MinReplicas: &minReplicas,
		MaxReplicas: &maxReplicas,
		NumOfHosts:  1,
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: workerContainers,
			},
		},
	})
}

func createPod(name string, namespace string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   namespace,
			Labels:      make(map[string]string),
			Annotations: make(map[string]string),
		},
	}
}

func podLabelsContains(pod *corev1.Pod, key string, value string) bool {
	if pod == nil {
		return false
	}

	if len(pod.Labels) > 0 {
		labelValue, exist := pod.Labels[key]
		if exist {
			if labelValue == value {
				return true
			}
		}
	}

	return false
}

func getTaskGroupsFromAnnotation(pod *corev1.Pod) ([]TaskGroup, error) {
	taskGroupInfo, exist := pod.Annotations[YuniKornTaskGroupsAnnotationName]
	if !exist {
		return nil, fmt.Errorf("not found")
	}

	taskGroups := []TaskGroup{}
	err := json.Unmarshal([]byte(taskGroupInfo), &taskGroups)
	if err != nil {
		return nil, err
	}
	// json.Unmarshal won't return error if name or MinMember is empty, but will return error if MinResource is empty or error format.
	for _, taskGroup := range taskGroups {
		if taskGroup.Name == "" {
			return nil, fmt.Errorf("can't get taskGroup Name from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinResource == nil {
			return nil, fmt.Errorf("can't get taskGroup MinResource from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember == int32(0) {
			return nil, fmt.Errorf("can't get taskGroup MinMember from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember < int32(0) {
			return nil, fmt.Errorf("minMember cannot be negative, %s",
				taskGroupInfo)
		}
	}
	return taskGroups, nil
}

func getTaskGroupsFromRayCluster(rayCluster *rayv1.RayCluster) ([]TaskGroup, error) {
	taskGroupInfo, exist := rayCluster.Annotations[YuniKornTaskGroupsAnnotationName]
	if !exist {
		return nil, fmt.Errorf("not found")
	}

	taskGroups := []TaskGroup{}
	err := json.Unmarshal([]byte(taskGroupInfo), &taskGroups)
	if err != nil {
		return nil, err
	}
	// json.Unmarshal won't return error if name or MinMember is empty, but will return error if MinResource is empty or error format.
	for _, taskGroup := range taskGroups {
		if taskGroup.Name == "" {
			return nil, fmt.Errorf("can't get taskGroup Name from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinResource == nil {
			return nil, fmt.Errorf("can't get taskGroup MinResource from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember == int32(0) {
			return nil, fmt.Errorf("can't get taskGroup MinMember from pod annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember < int32(0) {
			return nil, fmt.Errorf("minMember cannot be negative, %s",
				taskGroupInfo)
		}
	}
	return taskGroups, nil
}

func getTaskGroupsFromSubmitterPodTemplate(submitterPodTemplate *corev1.PodTemplateSpec) ([]TaskGroup, error) {
	taskGroupInfo, exist := submitterPodTemplate.Annotations[YuniKornTaskGroupsAnnotationName]
	if !exist {
		return nil, fmt.Errorf("not found")
	}

	taskGroups := []TaskGroup{}
	err := json.Unmarshal([]byte(taskGroupInfo), &taskGroups)
	if err != nil {
		return nil, err
	}
	// json.Unmarshal won't return error if name or MinMember is empty, but will return error if MinResource is empty or error format.
	for _, taskGroup := range taskGroups {
		if taskGroup.Name == "" {
			return nil, fmt.Errorf("can't get taskGroup Name from submitter pod template annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinResource == nil {
			return nil, fmt.Errorf("can't get taskGroup MinResource from submitter pod template annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember == int32(0) {
			return nil, fmt.Errorf("can't get taskGroup MinMember from submitter pod template annotation, %s",
				taskGroupInfo)
		}
		if taskGroup.MinMember < int32(0) {
			return nil, fmt.Errorf("minMember cannot be negative, %s",
				taskGroupInfo)
		}
	}
	return taskGroups, nil
}

func createSubmitterPodTemplate() *corev1.PodTemplateSpec {
	return &corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:  "ray-job-submitter",
					Image: "ray.io/ray-head:latest",
				},
			},
		},
	}
}
