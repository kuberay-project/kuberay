package e2e

import (
	"os/exec"
	"path"
	"regexp"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Directory when running test is kuberay/kubectl-plugin/test/e2e/
const (
	rayJobFilePath           = "./testdata/ray-job.interactive-mode.yaml"
	rayJobNoEnvFilePath      = "./testdata/ray-job.interactive-mode-no-runtime-env.yaml"
	kubectlRayJobWorkingDir  = "./testdata/rayjob-submit-working-dir/"
	entrypointSampleFileName = "entrypoint-python-sample.py"
	runtimeEnvSampleFileName = "runtime-env-sample.yaml"
	rayJobName               = "rayjob-sample"
)

var _ = Describe("Calling ray plugin `job submit` command on Ray Job", func() {
	var namespace string

	BeforeEach(func() {
		namespace = createTestNamespace()
		deployTestRayCluster(namespace)
		DeferCleanup(func() {
			deleteTestNamespace(namespace)
			namespace = ""
		})
	})

	It("succeed in submitting RayJob", func() {
		cmd := exec.Command(
			"kubectl", "ray", "job", "submit",
			"--namespace", namespace,
			"-f", rayJobFilePath,
			"--working-dir", kubectlRayJobWorkingDir,
			"--",
			"python", entrypointSampleFileName,
		)
		output, err := cmd.CombinedOutput()

		Expect(err).NotTo(HaveOccurred())
		// Retrieve the Job ID from the output
		cmdOutputJobID := extractRayJobID(string(output))

		// Use kubectl to check status of the rayjob
		getAndCheckRayJob(namespace, rayJobName, cmdOutputJobID, "SUCCEEDED", "Complete")
	})

	It("succeed in submitting RayJob with runtime environment set with working dir", func() {
		runtimeEnvFilePath := path.Join(kubectlRayJobWorkingDir, runtimeEnvSampleFileName)
		cmd := exec.Command(
			"kubectl", "ray", "job", "submit",
			"--namespace", namespace, "-f",
			rayJobNoEnvFilePath, "--runtime-env",
			runtimeEnvFilePath,
			"--",
			"python", entrypointSampleFileName,
		)

		output, err := cmd.CombinedOutput()

		Expect(err).NotTo(HaveOccurred())
		// Retrieve the Job ID from the output
		cmdOutputJobID := extractRayJobID(string(output))

		// Use kubectl to check status of the rayjob
		getAndCheckRayJob(namespace, rayJobName, cmdOutputJobID, "SUCCEEDED", "Complete")
	})

	It("succeed in submitting RayJob with headNodeSelectors and workerNodeSelectors", func() {
		runtimeEnvFilePath := path.Join(kubectlRayJobWorkingDir, runtimeEnvSampleFileName)

		cmd := exec.Command(
			"kubectl", "ray", "job", "submit",
			"--namespace", namespace,
			"--name", rayJobName,
			"--runtime-env", runtimeEnvFilePath,
			"--head-cpu", "1",
			"--head-memory", "2Gi",
			"--worker-cpu", "1",
			"--worker-memory", "2Gi",
			"--head-node-selectors", "kubernetes.io/os=linux",
			"--worker-node-selectors", "kubernetes.io/os=linux",
			"--",
			"python",
			entrypointSampleFileName,
		)
		output, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		// Retrieve the Job ID from the output
		cmdOutputJobID := extractRayJobID(string(output))

		rayJob := getAndCheckRayJob(namespace, rayJobName, cmdOutputJobID, "SUCCEEDED", "Complete")
		// Retrieve Job Head Node Selectors
		Expect(rayJob.Spec.RayClusterSpec.HeadGroupSpec.Template.Spec.NodeSelector["kubernetes.io/os"]).To(Equal("linux"))
		// Retrieve Job Worker Node Selectors
		Expect(rayJob.Spec.RayClusterSpec.WorkerGroupSpecs[0].Template.Spec.NodeSelector["kubernetes.io/os"]).To(Equal("linux"))
	})

	It("successfully submits RayJob with TTL 0 using CLI flags only (no YAML config)", func() {
		runtimeEnvFilePath := path.Join(kubectlRayJobWorkingDir, runtimeEnvSampleFileName)
		cmd := exec.Command(
			"kubectl", "ray", "job", "submit",
			"--namespace", namespace,
			"--name", rayJobName,
			"--runtime-env", runtimeEnvFilePath,
			"--ttl-seconds-after-finished", "0",
			"--head-cpu", "1",
			"--head-memory", "2Gi",
			"--worker-cpu", "1",
			"--worker-memory", "2Gi",
			"--",
			"python",
			entrypointSampleFileName,
		)
		output, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred(), string(output))
		// Retrieve the Job ID from the output
		cmdOutputJobID := extractRayJobID(string(output))

		rayJob := getAndCheckRayJob(namespace, rayJobName, cmdOutputJobID, "SUCCEEDED", "Complete")
		Expect(rayJob.Spec.TTLSecondsAfterFinished).To(Equal(int32(0)))
		Expect(rayJob.Spec.ShutdownAfterJobFinishes).To(BeTrue())
	})

	It("successfully submits RayJob without TTL", func() {
		runtimeEnvFilePath := path.Join(kubectlRayJobWorkingDir, runtimeEnvSampleFileName)
		cmd := exec.Command(
			"kubectl", "ray", "job", "submit",
			"--namespace", namespace,
			"--name", rayJobName,
			"--runtime-env", runtimeEnvFilePath,
			"--head-cpu", "1",
			"--head-memory", "2Gi",
			"--worker-cpu", "1",
			"--worker-memory", "2Gi",
			"--",
			"python",
			entrypointSampleFileName,
		)
		output, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		// Retrieve the Job ID from the output
		cmdOutputJobID := extractRayJobID(string(output))

		rayJob := getAndCheckRayJob(namespace, rayJobName, cmdOutputJobID, "SUCCEEDED", "Complete")
		Expect(rayJob.Spec.TTLSecondsAfterFinished).To(Equal(int32(0)))
		Expect(rayJob.Spec.ShutdownAfterJobFinishes).To(BeFalse())
	})

	It("succeed in submitting RayJob with ttl-seconds-after-finished set to 10 with yaml config", func() {
		cmd := exec.Command(
			"kubectl", "ray", "job", "submit",
			"--namespace", namespace,
			"-f", rayJobFilePath,
			"--working-dir", kubectlRayJobWorkingDir,
			"--ttl-seconds-after-finished", "10",
			"--",
			"python",
			entrypointSampleFileName,
		)
		output, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		// Retrieve the Job ID from the output
		cmdOutputJobID := extractRayJobID(string(output))

		rayJob := getAndCheckRayJob(namespace, rayJobName, cmdOutputJobID, "SUCCEEDED", "Complete")
		Expect(rayJob.Spec.TTLSecondsAfterFinished).To(Equal(int32(10)))
		Expect(rayJob.Spec.ShutdownAfterJobFinishes).To(BeTrue())
	})
	It("failure in submitting RayJob with ttl-seconds-after-finished set to -5 with yaml config", func() {
		cmd := exec.Command(
			"kubectl", "ray", "job", "submit",
			"--namespace", namespace,
			"-f", rayJobFilePath,
			"--working-dir", kubectlRayJobWorkingDir,
			"--ttl-seconds-after-finished", "-5",
			"--",
			"python",
			entrypointSampleFileName,
		)
		output, err := cmd.CombinedOutput()
		Expect(err).To(HaveOccurred())
		Expect(string(output)).To(ContainSubstring("Error: --ttl-seconds-after-finished must be greater than or equal to 0"))
	})
})

// `extractRayJobID` extracts the Ray Job ID from the output of `kubectl ray job submit`.
//
// Use regex to extract the job ID from the output.
// The output is expected to be like:
//
//	Current status: RUNNING (RayJob: rayjob-sample, JobID: raysubmit_XBBxrNHztQa1rHpL)
//	Current status: SUCCEEDED (RayJob: rayjob-sample, JobID: raysubmit_XBBxrNHztQa1rHpL)
//	Job raysubmit_XBBxrNHztQa1rHpL finished with status SUCCEEDED.
//
// It returns the first matched Job ID string like "raysubmit_XBBxrNHztQa1rHpL".
func extractRayJobID(output string) string {
	// Match any string that starts with "raysubmit_" followed by alphanumeric characters.
	regexExp := regexp.MustCompile(`raysubmit_[a-zA-Z0-9]+`)

	// Find the first match in the output
	matches := regexExp.FindStringSubmatch(output)

	// Return the matched Job ID if found, otherwise return an empty string
	if len(matches) >= 1 {
		return matches[0]
	}
	return ""
}
