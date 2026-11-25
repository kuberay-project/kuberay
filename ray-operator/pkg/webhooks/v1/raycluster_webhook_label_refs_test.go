package v1

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("RayCluster labelRefs", func() {
	It("accepts allowlisted mappings and rejects the rest", func() {
		accepted := newLabelRefsRayCluster(nodeLabelRef(testConfig.AllowedNodeLabels[0], "ray.io/zone"))
		Expect(k8sClient.Create(ctx, accepted)).To(Succeed())
		DeferCleanup(k8sClient.Delete, ctx, accepted)

		err := k8sClient.Create(ctx, newLabelRefsRayCluster(nodeLabelRef("cloud.google.com/gke-nodepool", "")))
		Expect(err).To(MatchError(ContainSubstring(`node label "cloud.google.com/gke-nodepool" is not in the operator's allowedNodeLabels`)))
	})
})
