package support

import (
	"embed"
	"fmt"
	"os"
	"time"

	"github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
)

var (
	//go:embed *.py
	_files embed.FS

	TestApplyOptions = metav1.ApplyOptions{FieldManager: "kuberay-test", Force: true}

	TestTimeoutShort  = 1 * time.Minute
	TestTimeoutMedium = 2 * time.Minute
	TestTimeoutLong   = 5 * time.Minute
)

type SupportOption[T any] func(t *T) *T

func Apply[T any](t *T, options ...SupportOption[T]) *T {
	for _, opt := range options {
		t = opt(t)
	}
	return t
}

func options[T any](options ...SupportOption[T]) SupportOption[T] {
	return func(t *T) *T {
		for _, opt := range options {
			t = opt(t)
		}
		return t
	}
}

func ReadFile(t Test, fileName string) []byte {
	t.T().Helper()
	file, err := _files.ReadFile(fileName)
	require.NoError(t.T(), err)
	return file
}

func NewConfigMap(namespace string, options ...SupportOption[corev1ac.ConfigMapApplyConfiguration]) *corev1ac.ConfigMapApplyConfiguration {
	cmAC := corev1ac.ConfigMap("jobs", namespace).
		WithBinaryData(map[string][]byte{}).
		WithImmutable(true)

	return ConfigMapWith(cmAC, options...)
}

func ConfigMapWith(configMapAC *corev1ac.ConfigMapApplyConfiguration, options ...SupportOption[corev1ac.ConfigMapApplyConfiguration]) *corev1ac.ConfigMapApplyConfiguration {
	return Apply(configMapAC, options...)
}

func file(t Test, fileName string) SupportOption[corev1ac.ConfigMapApplyConfiguration] {
	return func(cmAC *corev1ac.ConfigMapApplyConfiguration) *corev1ac.ConfigMapApplyConfiguration {
		cmAC.WithBinaryData(map[string][]byte{fileName: ReadFile(t, fileName)})
		return cmAC
	}
}

func Files(t Test, fileNames ...string) SupportOption[corev1ac.ConfigMapApplyConfiguration] {
	var files []SupportOption[corev1ac.ConfigMapApplyConfiguration]
	for _, fileName := range fileNames {
		files = append(files, file(t, fileName))
	}
	return options(files...)
}

func NewRayClusterSpec(options ...SupportOption[rayv1ac.RayClusterSpecApplyConfiguration]) *rayv1ac.RayClusterSpecApplyConfiguration {
	return RayClusterSpecWith(rayClusterSpec(), options...)
}

func RayClusterSpecWith(spec *rayv1ac.RayClusterSpecApplyConfiguration, options ...SupportOption[rayv1ac.RayClusterSpecApplyConfiguration]) *rayv1ac.RayClusterSpecApplyConfiguration {
	return Apply(spec, options...)
}

func MountConfigMap[T rayv1ac.RayClusterSpecApplyConfiguration | corev1ac.PodTemplateSpecApplyConfiguration](configMap *corev1.ConfigMap, mountPath string) SupportOption[T] {
	return func(t *T) *T {
		switch obj := (any)(t).(type) {
		case *rayv1ac.RayClusterSpecApplyConfiguration:
			obj.HeadGroupSpec.Template.Spec.Containers[0].WithVolumeMounts(corev1ac.VolumeMount().
				WithName(configMap.Name).
				WithMountPath(mountPath))
			obj.HeadGroupSpec.Template.Spec.WithVolumes(corev1ac.Volume().
				WithName(configMap.Name).
				WithConfigMap(corev1ac.ConfigMapVolumeSource().WithName(configMap.Name)))

		case *corev1ac.PodTemplateSpecApplyConfiguration:
			obj.Spec.Containers[0].WithVolumeMounts(corev1ac.VolumeMount().
				WithName(configMap.Name).
				WithMountPath(mountPath))
			obj.Spec.WithVolumes(corev1ac.Volume().
				WithName(configMap.Name).
				WithConfigMap(corev1ac.ConfigMapVolumeSource().WithName(configMap.Name)))
		}
		return t
	}
}

func rayClusterSpec() *rayv1ac.RayClusterSpecApplyConfiguration {
	return rayv1ac.RayClusterSpec().
		WithRayVersion(GetRayVersion()).
		WithHeadGroupSpec(rayv1ac.HeadGroupSpec().
			WithRayStartParams(map[string]string{"dashboard-host": "0.0.0.0"}).
			WithTemplate(HeadPodTemplateApplyConfiguration())).
		WithWorkerGroupSpecs(rayv1ac.WorkerGroupSpec().
			WithReplicas(1).
			WithMinReplicas(1).
			WithMaxReplicas(1).
			WithGroupName("small-group").
			WithRayStartParams(map[string]string{"num-cpus": "1"}).
			WithTemplate(WorkerPodTemplateApplyConfiguration()))
}

func PodTemplateSpecApplyConfiguration(template *corev1ac.PodTemplateSpecApplyConfiguration, options ...SupportOption[corev1ac.PodTemplateSpecApplyConfiguration]) *corev1ac.PodTemplateSpecApplyConfiguration {
	return Apply(template, options...)
}

func HeadPodTemplateApplyConfiguration() *corev1ac.PodTemplateSpecApplyConfiguration {
	return corev1ac.PodTemplateSpec().
		WithSpec(corev1ac.PodSpec().
			WithContainers(corev1ac.Container().
				WithName("ray-head").
				WithImage(GetRayImage()).
				WithPorts(
					corev1ac.ContainerPort().WithName(utils.GcsServerPortName).WithContainerPort(utils.DefaultGcsServerPort),
					corev1ac.ContainerPort().WithName(utils.ServingPortName).WithContainerPort(utils.DefaultServingPort),
					corev1ac.ContainerPort().WithName(utils.DashboardPortName).WithContainerPort(utils.DefaultDashboardPort),
					corev1ac.ContainerPort().WithName(utils.ClientPortName).WithContainerPort(utils.DefaultClientPort),
				).
				WithResources(corev1ac.ResourceRequirements().
					WithRequests(corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("300m"),
						corev1.ResourceMemory: resource.MustParse("1G"),
					}).
					WithLimits(corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("2G"),
					}))))
}

func WorkerPodTemplateApplyConfiguration() *corev1ac.PodTemplateSpecApplyConfiguration {
	return corev1ac.PodTemplateSpec().
		WithSpec(corev1ac.PodSpec().
			WithContainers(corev1ac.Container().
				WithName("ray-worker").
				WithImage(GetRayImage()).
				WithResources(corev1ac.ResourceRequirements().
					WithRequests(corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("300m"),
						corev1.ResourceMemory: resource.MustParse("1G"),
					}).
					WithLimits(corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("1G"),
					}))))
}

func JobSubmitterPodTemplateApplyConfiguration() *corev1ac.PodTemplateSpecApplyConfiguration {
	return corev1ac.PodTemplateSpec().
		WithSpec(corev1ac.PodSpec().
			WithRestartPolicy(corev1.RestartPolicyNever).
			WithContainers(corev1ac.Container().
				WithName("ray-job-submitter").
				WithImage(GetRayImage()).
				WithResources(corev1ac.ResourceRequirements().
					WithRequests(corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("200m"),
						corev1.ResourceMemory: resource.MustParse("200Mi"),
					}).
					WithLimits(corev1.ResourceList{
						corev1.ResourceCPU:    resource.MustParse("500m"),
						corev1.ResourceMemory: resource.MustParse("500Mi"),
					}))))
}

func init() {
	if value, ok := os.LookupEnv("KUBERAY_TEST_TIMEOUT_SHORT"); ok {
		if duration, err := time.ParseDuration(value); err == nil {
			TestTimeoutShort = duration
		} else {
			fmt.Printf("Error parsing KUBERAY_TEST_TIMEOUT_SHORT. Using default value: %s", TestTimeoutShort)
		}
	}
	if value, ok := os.LookupEnv("KUBERAY_TEST_TIMEOUT_MEDIUM"); ok {
		if duration, err := time.ParseDuration(value); err == nil {
			TestTimeoutMedium = duration
		} else {
			fmt.Printf("Error parsing KUBERAY_TEST_TIMEOUT_MEDIUM. Using default value: %s", TestTimeoutMedium)
		}
	}
	if value, ok := os.LookupEnv("KUBERAY_TEST_TIMEOUT_LONG"); ok {
		if duration, err := time.ParseDuration(value); err == nil {
			TestTimeoutLong = duration
		} else {
			fmt.Printf("Error parsing KUBERAY_TEST_TIMEOUT_LONG. Using default value: %s", TestTimeoutLong)
		}
	}

	// Gomega settings
	gomega.SetDefaultEventuallyTimeout(TestTimeoutShort)
	gomega.SetDefaultEventuallyPollingInterval(1 * time.Second)
	gomega.SetDefaultConsistentlyDuration(30 * time.Second)
	gomega.SetDefaultConsistentlyPollingInterval(1 * time.Second)
	// Disable object truncation on test results
	format.MaxLength = 0
}

func IsPodRunningAndReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady && condition.Status == corev1.ConditionTrue {
			return true
		}
	}
	return false
}

func AllPodsRunningAndReady(pods []corev1.Pod) bool {
	for _, pod := range pods {
		if !IsPodRunningAndReady(&pod) {
			return false
		}
	}
	return true
}

func DeletePodAndWait(test Test, rayCluster *rayv1.RayCluster, namespace *corev1.Namespace, currentHeadPod *corev1.Pod) (*corev1.Pod, error) {
	g := gomega.NewWithT(test.T())

	err := test.Client().Core().CoreV1().Pods(namespace.Name).Delete(test.Ctx(), currentHeadPod.Name, metav1.DeleteOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to delete head pod %s: %w", currentHeadPod.Name, err)
	}

	PodUID := func(p *corev1.Pod) string { return string(p.UID) }

	// Wait for a new head pod to be created (different UID)
	g.Eventually(HeadPod(test, rayCluster), TestTimeoutMedium).
		ShouldNot(gomega.WithTransform(PodUID, gomega.Equal(string(currentHeadPod.UID))),
			"New head pod should have different UID than the deleted one")

	g.Eventually(HeadPod(test, rayCluster), TestTimeoutMedium).
		Should(gomega.WithTransform(func(p *corev1.Pod) string { return string(p.Status.Phase) }, gomega.Equal("Running")),
			"New head pod should be in Running state")

	newHeadPod, err := GetHeadPod(test, rayCluster)
	if err != nil {
		return nil, fmt.Errorf("failed to get new head pod: %w", err)
	}

	return newHeadPod, nil
}
