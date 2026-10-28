package e2e

import (
	"strings"
	"testing"
	"time"

	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	rayv1ac "github.com/ray-project/kuberay/ray-operator/pkg/client/applyconfiguration/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestEventForwarder(t *testing.T) {
	test := With(t)
	g := NewWithT(t)

	namespace := test.NewTestNamespace()

	test.T().Run("Forward Kubernetes Node warning event to RayCluster", func(t *testing.T) {
		t.Parallel()

		rayClusterAC := rayv1ac.RayCluster("raycluster-event-e2e", namespace.Name).
			WithSpec(NewRayClusterSpec().
				WithManagedBy(utils.KubeRayController))

		rayCluster, err := test.Client().Ray().RayV1().RayClusters(namespace.Name).Apply(test.Ctx(), rayClusterAC, TestApplyOptions)
		g.Expect(err).NotTo(HaveOccurred())
		LogWithTimestamp(test.T(), "Created RayCluster %s/%s successfully", rayCluster.Namespace, rayCluster.Name)

		LogWithTimestamp(test.T(), "Waiting for Head pod of RayCluster %s/%s to be running and ready", rayCluster.Namespace, rayCluster.Name)
		g.Eventually(HeadPod(test, rayCluster), TestTimeoutMedium).
			Should(WithTransform(IsPodRunningAndReady, BeTrue()))

		headPod, err := GetHeadPod(test, rayCluster)
		g.Expect(err).NotTo(HaveOccurred())
		g.Expect(headPod).NotTo(BeNil())

		LogWithTimestamp(test.T(), "Head pod %s scheduled on node %s", headPod.Name, headPod.Spec.NodeName)

		now := metav1.Now()
		fakeNodeEvent := &corev1.Event{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "e2e-node-event-",
				Namespace:    "default",
			},
			InvolvedObject: corev1.ObjectReference{
				Kind: "Node",
				Name: headPod.Spec.NodeName,
			},
			Reason:         "MemoryPressure",
			Message:        "Simulated e2e node memory pressure warning",
			Type:           corev1.EventTypeWarning,
			FirstTimestamp: now,
			LastTimestamp:  now,
			Count:          1,
			Source: corev1.EventSource{
				Component: "kubelet",
				Host:      headPod.Spec.NodeName,
			},
		}

		_, err = test.Client().Core().CoreV1().Events("default").Create(test.Ctx(), fakeNodeEvent, metav1.CreateOptions{})
		g.Expect(err).NotTo(HaveOccurred())

		LogWithTimestamp(test.T(), "Created fake Node event targeting node %s", headPod.Spec.NodeName)

		g.Eventually(func(g Gomega) bool {
			events, err := test.Client().Core().CoreV1().Events(namespace.Name).List(test.Ctx(), metav1.ListOptions{})
			g.Expect(err).NotTo(HaveOccurred())

			for _, ev := range events.Items {
				if ev.InvolvedObject.Kind == "RayCluster" &&
					ev.InvolvedObject.Name == rayCluster.Name &&
					ev.Reason == "MemoryPressure/kubelet" &&
					strings.Contains(ev.Message, "MemoryPressure") &&
					strings.Contains(ev.Message, headPod.Spec.NodeName) {
					return true
				}
			}
			return false
		}, TestTimeoutMedium, 2*time.Second).Should(BeTrue(), "Expected RayCluster to receive forwarded Node event")
	})
}
