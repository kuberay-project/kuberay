package v1

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

// TestRayStartWithNodeLabelsShell runs the generated shell fragment with `ray start` replaced by `echo`
// 2 runs: no labels, good label value
func TestRayStartWithNodeLabelsShell(t *testing.T) {
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash is not available")
	}
	t.Cleanup(func() { _ = os.Remove(utils.RayNodeLabelsFilePath) })

	script := rayStartWithNodeLabels("echo RAYSTART")
	run := func(labelsJSON *string) (string, error) {
		cmd := exec.CommandContext(t.Context(), bash, "-c", "--", script)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH")}
		if labelsJSON != nil {
			cmd.Env = append(cmd.Env, utils.RAY_NODE_LABELS_JSON+"="+*labelsJSON)
		}
		out, err := cmd.CombinedOutput()
		return string(out), err
	}

	out, err := run(nil)
	require.Error(t, err, "missing labels must fail before ray start")
	assert.Contains(t, out, "is empty")

	good := `{"ray.io/zone":"us-central1-a"}`
	out, err = run(&good)
	require.NoError(t, err, out)
	assert.Contains(t, out, "RAYSTART --labels-file="+utils.RayNodeLabelsFilePath)
	content, err := os.ReadFile(utils.RayNodeLabelsFilePath)
	require.NoError(t, err)
	assert.Equal(t, good+"\n", string(content))
}

func TestPrepareNodeLabelDelivery(t *testing.T) {
	pod := newWorkerPod("test")
	require.NoError(t, prepareNodeLabelDelivery(pod))
	container := pod.Spec.Containers[utils.RayContainerIndex]
	pointer := nodeLabelsEnv(container.Env)
	require.NotNil(t, pointer)
	assert.Equal(t, "metadata.annotations['"+utils.RayNodeLabelsAnnotationKey+"']", pointer.ValueFrom.FieldRef.FieldPath)
	assert.Equal(t, testUlimitCmd+rayStartWithNodeLabels(testRayStartCmd), container.Args[0])

	t.Run("reinvocation leaves the pod unchanged", func(t *testing.T) {
		before := pod.DeepCopy()
		require.NoError(t, prepareNodeLabelDelivery(pod))
		assert.Equal(t, before, pod)
	})

	refused := []struct {
		name   string
		mutate func(*corev1.PodSpec)
		err    string
	}{
		{"no containers", func(s *corev1.PodSpec) { s.Containers = nil }, "no Ray container"},
		{"user-set pointer", func(s *corev1.PodSpec) {
			s.Containers[0].Env = append(s.Containers[0].Env, corev1.EnvVar{Name: utils.RAY_NODE_LABELS_JSON, Value: "{}"})
		}, "managed by KubeRay"},
		{"missing generated command", func(s *corev1.PodSpec) { s.Containers[0].Env = nil }, utils.KUBERAY_GEN_RAY_START_CMD},
		{"generated command missing from args", func(s *corev1.PodSpec) { s.Containers[0].Args = []string{"echo no ray here"} }, "was not found"},
	}
	for _, tc := range refused {
		t.Run(tc.name+" is refused", func(t *testing.T) {
			pod := newWorkerPod("test")
			tc.mutate(&pod.Spec)
			require.ErrorContains(t, prepareNodeLabelDelivery(pod), tc.err)
		})
	}
}

func TestPodWebhookDefault(t *testing.T) {
	features.SetFeatureGateDuringTest(t, features.NodeLabelDelivery, true)
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, rayv1.AddToScheme(scheme))
	webhook := &PodWebhook{Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(newLabelRefsCluster()).Build()}

	unchanged := func(t *testing.T, pod *corev1.Pod) {
		before := pod.DeepCopy()
		require.NoError(t, webhook.Default(ctx, pod))
		assert.Equal(t, before, pod)
	}

	t.Run("disabled feature gate leaves the pod alone", func(t *testing.T) {
		features.SetFeatureGateDuringTest(t, features.NodeLabelDelivery, false)
		unchanged(t, newWorkerPod("test"))
	})

	t.Run("head pod is left alone", func(t *testing.T) {
		pod := newWorkerPod("test")
		pod.Labels[utils.RayNodeTypeLabelKey] = string(rayv1.HeadNode)
		unchanged(t, pod)
	})

	t.Run("worker of a group without label mappings is left alone", func(t *testing.T) {
		unchanged(t, newWorkerPod("plain"))
		unchanged(t, newWorkerPod("empty"))
	})

	t.Run("worker of a labelRefs group is prepared", func(t *testing.T) {
		pod := newWorkerPod("test")
		require.NoError(t, webhook.Default(ctx, pod))
		assert.NotNil(t, nodeLabelsEnv(pod.Spec.Containers[utils.RayContainerIndex].Env))
	})

	t.Run("unknown RayCluster refuses the pod", func(t *testing.T) {
		pod := newWorkerPod("test")
		pod.Labels[utils.RayClusterLabelKey] = "missing"
		require.ErrorContains(t, webhook.Default(ctx, pod), "cannot read RayCluster default/missing")
	})

	t.Run("unknown worker group refuses the pod", func(t *testing.T) {
		require.ErrorContains(t, webhook.Default(ctx, newWorkerPod("ghost")), "worker group ghost not found")
	})
}
