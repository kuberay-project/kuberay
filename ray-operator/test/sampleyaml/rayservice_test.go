package sampleyaml

import (
	"path"
	"testing"

	. "github.com/onsi/gomega"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	. "github.com/ray-project/kuberay/ray-operator/test/support"
)

func TestRayService(t *testing.T) {
	tests := []struct {
		name string
	}{
		{
			name: "ray-service.custom-serve-service.yaml",
		},
		{
			name: "ray-service.different-port.yaml",
		},
		{
			name: "ray-service.high-availability.yaml",
		},
		{
			name: "ray-service.sample.yaml",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			test := With(t)
			g := NewWithT(t)
			g.ConfigureWithT(WithRayServiceResourceLogger(test))

			yamlFilePath := path.Join(GetSampleYAMLDir(test), tt.name)
			namespace := test.NewTestNamespace()
			rayServiceFromYaml := DeserializeRayServiceYAML(test, yamlFilePath)
			KubectlApplyYAML(test, yamlFilePath, namespace.Name)

			rayService, err := GetRayService(test, namespace.Name, rayServiceFromYaml.Name)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rayService).NotTo(BeNil())

			LogWithTimestamp(test.T(), "Waiting for RayService %s/%s to be running", rayService.Namespace, rayService.Name)
			g.Eventually(RayService(test, rayService.Namespace, rayService.Name), TestTimeoutMedium).
				Should(WithTransform(RayServiceStatus, Equal(rayv1.Running)))
			// Get the latest rayService
			rayService, err = GetRayService(test, namespace.Name, rayServiceFromYaml.Name)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(rayService).NotTo(BeNil())

			rayClusterName := rayService.Status.ActiveServiceStatus.RayClusterName

			rayCluster, err := GetRayCluster(test, namespace.Name, rayClusterName)
			g.Expect(err).NotTo(HaveOccurred())

			// Check if the head pod is ready
			g.Eventually(HeadPod(test, rayCluster), TestTimeoutShort).Should(WithTransform(IsPodRunningAndReady, BeTrue()))

			// Check if .status.numServeEndpoints is greater than zero
			g.Eventually(func(g Gomega) int32 {
				rs, err := GetRayService(test, namespace.Name, rayServiceFromYaml.Name)
				g.Expect(err).NotTo(HaveOccurred())
				return rs.Status.NumServeEndpoints
			}, TestTimeoutShort).Should(BeNumerically(">", 0))

			// Check if all applications are running
			g.Eventually(RayService(test, namespace.Name, rayServiceFromYaml.Name), TestTimeoutMedium).Should(WithTransform(AllAppsRunning, BeTrue()))
			// Query dashboard to get the serve application status in head pod
			g.Eventually(QueryDashboardGetAppStatus(test, rayCluster), TestTimeoutShort).Should(Succeed())
		})
	}
}
