package v1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"

	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

var _ = Describe("Pod labelRefs", func() {
	It("prepares a labelRefs worker at CREATE", func() {
		cluster := newLabelRefsRayCluster(nodeLabelRef("topology.kubernetes.io/zone", ""))
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, cluster)

		pod := newWorkerPod("test")
		pod.Name = cluster.Name + "-test-worker-" + rand.String(5)
		pod.Labels[utils.RayClusterLabelKey] = cluster.Name
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, pod)

		// the unit tests cover the mutation itself; this proves the real admission path applied it
		Expect(nodeLabelsEnv(pod.Spec.Containers[utils.RayContainerIndex].Env)).NotTo(BeNil())
	})

	It("delivers the node labels onto the pod at bind time", func() {
		cluster := newLabelRefsRayCluster(nodeLabelRef("topology.kubernetes.io/zone", ""))
		Expect(k8sClient.Create(ctx, cluster)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, cluster)

		pod := newWorkerPod("test")
		pod.Name = cluster.Name + "-test-worker-" + rand.String(5)
		pod.Labels[utils.RayClusterLabelKey] = cluster.Name
		Expect(k8sClient.Create(ctx, pod)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, pod)

		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: cluster.Name + "-node", Labels: map[string]string{"topology.kubernetes.io/zone": "us-central1-a"}}}
		Expect(k8sClient.Create(ctx, node)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, node)

		binding := &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: "default"}, Target: corev1.ObjectReference{Kind: "Node", Name: node.Name}}
		Expect(k8sClient.SubResource("binding").Create(ctx, pod, binding)).To(Succeed())

		// the API server copies Binding annotations onto the pod together with spec.nodeName
		bound := &corev1.Pod{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: pod.Name}, bound)).To(Succeed())
		Expect(bound.Spec.NodeName).To(Equal(node.Name))
		Expect(bound.Annotations).To(HaveKeyWithValue(utils.RayNodeLabelsAnnotationKey, `{"topology.kubernetes.io/zone":"us-central1-a"}`))
	})
})
