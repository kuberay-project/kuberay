package e2e

import (
	"os/exec"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Calling ray plugin `scale cluster` command to modify worker groups", func() {
	var namespace string
	clusterName := "raycluster-kuberay"
	workergroupName := "workergroup"

	BeforeEach(func() {
		namespace = createTestNamespace()
		deployTestRayCluster(namespace)
		DeferCleanup(func() {
			deleteTestNamespace(namespace)
			namespace = ""
		})
	})

	It("succeed in scaling the replicas of the ray cluster correctly", func() {
		cmd := exec.Command("kubectl", "ray", "scale", "cluster", clusterName, "--worker-group", workergroupName,
			"--namespace", namespace, "--replicas", "3")
		_, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred())

		minOut, maxOut, replicas := getWorkerGroupValues(namespace, clusterName, workergroupName)

		Expect(minOut).To(Equal("1"))
		Expect(maxOut).To(Equal("5"))
		Expect(replicas).To(Equal("3"))
	})

	It("succeed in scaling the min-replicas of the ray cluster correctly", func() {
		cmd := exec.Command("kubectl", "ray", "scale", "cluster", clusterName, "--worker-group", workergroupName,
			"--namespace", namespace, "--replicas", "3")
		_, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred())

		cmd = exec.Command("kubectl", "ray", "scale", "cluster", clusterName, "--worker-group", workergroupName,
			"--namespace", namespace, "--min-replicas", "2")
		_, err = cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred())

		minOut, maxOut, replicas := getWorkerGroupValues(namespace, clusterName, workergroupName)

		Expect(minOut).To(Equal("2"))
		Expect(maxOut).To(Equal("5"))
		Expect(replicas).To(Equal("3"))
	})

	It("succeed in scaling the max-replicas of the ray cluster correctly", func() {
		cmd := exec.Command("kubectl", "ray", "scale", "cluster", clusterName, "--worker-group", workergroupName,
			"--namespace", namespace, "--max-replicas", "7")
		_, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred())

		minOut, maxOut, replicas := getWorkerGroupValues(namespace, clusterName, workergroupName)

		Expect(minOut).To(Equal("1"))
		Expect(maxOut).To(Equal("7"))
		Expect(replicas).To(Equal("1"))
	})

	It("succeed in scaling the replicas, min-replicas and max-replicas of the ray cluster correctly", func() {
		cmd := exec.Command("kubectl", "ray", "scale", "cluster", clusterName, "--worker-group", workergroupName,
			"--namespace", namespace, "--replicas", "3", "--min-replicas", "2", "--max-replicas", "7")
		_, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred())

		minOut, maxOut, replicas := getWorkerGroupValues(namespace, clusterName, workergroupName)

		Expect(minOut).To(Equal("2"))
		Expect(maxOut).To(Equal("7"))
		Expect(replicas).To(Equal("3"))
	})

	It("should not succeed when cluster does not exist", func() {
		cmd := exec.Command("kubectl", "ray", "scale", "cluster",
			"fakeclustername",
			"--namespace", namespace,
			"--worker-group", workergroupName,
			"--replicas", "3")

		output, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(output)).To(ContainSubstring("not found"))
	})

	It("should not succeed when workergroup does not exist", func() {
		cmd := exec.Command("kubectl", "ray", "scale", "cluster",
			clusterName,
			"--namespace", namespace,
			"--worker-group", "fakeworkergroupName",
			"--replicas", "3")

		output, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(output)).To(ContainSubstring("not found"))
	})

	It("should not succeed when scaling min-replicas greater than max-replicas", func() {
		cmd := exec.Command("kubectl", "ray", "scale", "cluster",
			clusterName,
			"--namespace", namespace,
			"--worker-group", workergroupName,
			"--min-replicas", "999")

		output, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(output)).To(ContainSubstring("greater than"))
	})

	It("should not succeed when scaling max-replicas less than min-replicas", func() {
		cmd := exec.Command("kubectl", "ray", "scale", "cluster",
			clusterName,
			"--namespace", namespace,
			"--worker-group", workergroupName,
			"--max-replicas", "0")

		output, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(output)).To(ContainSubstring("greater than"))
	})

	It("should not succeed when scaling replicas less than min-replicas", func() {
		cmd := exec.Command("kubectl", "ray", "scale", "cluster",
			clusterName,
			"--namespace", namespace,
			"--worker-group", workergroupName,
			"--replicas", "0")

		output, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(output)).To(ContainSubstring("less than"))
	})

	It("should not succeed when scaling replicas greater than max-replicas", func() {
		cmd := exec.Command("kubectl", "ray", "scale", "cluster",
			clusterName,
			"--namespace", namespace,
			"--worker-group", workergroupName,
			"--replicas", "999")

		output, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(output)).To(ContainSubstring("greater than"))
	})
})
