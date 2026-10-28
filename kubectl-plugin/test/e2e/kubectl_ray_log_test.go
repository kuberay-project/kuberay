package e2e

import (
	"maps"
	"os"
	"os/exec"
	"path"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var requiredFileSet = map[string]string{
	"stdout.log": "Ray runtime started",
	"raylet.out": "Ray Event initialized for RAYLET",
}

var _ = Describe("Calling ray plugin `log` command on Ray Cluster", func() {
	var namespace string

	BeforeEach(func() {
		namespace = createTestNamespace()
		deployTestRayCluster(namespace)
		DeferCleanup(func() {
			deleteTestNamespace(namespace)
			namespace = ""
		})
	})

	It("succeed in retrieving all ray cluster logs", func() {
		expectedDirPath := "./raycluster-kuberay"
		expectedOutputStringFormat := `Command set to retrieve both head and worker node logs\.\nNo output directory specified, creating dir under current directory using resource name\.\nDownloading log for Ray Node raycluster-kuberay-head-\w+\nDownloading log for Ray Node raycluster-kuberay-workergroup-worker-\w+`

		cmd := exec.Command("kubectl", "ray", "log", "--namespace", namespace, "raycluster-kuberay", "--node-type", "all")
		output, err := cmd.CombinedOutput()

		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(output))).Should(MatchRegexp(expectedOutputStringFormat))

		// Check that the log directory exists
		logDirInfo, err := os.Stat(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(logDirInfo.IsDir()).To(BeTrue())

		// Check the contents of the cluster directory
		fileList, err := os.ReadDir(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(fileList).To(HaveLen(2))

		for _, file := range fileList {
			Expect(file.IsDir()).To(BeTrue())

			// Check that the files exist and have correct content
			logList, err := os.ReadDir(path.Join(expectedDirPath, file.Name()))
			Expect(err).NotTo(HaveOccurred())

			currentRequiredFileList := make(map[string]string)

			maps.Copy(currentRequiredFileList, requiredFileSet)

			for _, logFile := range logList {
				if checkContent := currentRequiredFileList[logFile.Name()]; checkContent != "" {
					delete(currentRequiredFileList, logFile.Name())

					// read and check file content
					fileContentByte, err := os.ReadFile(path.Join(expectedDirPath, file.Name(), logFile.Name()))
					Expect(err).NotTo(HaveOccurred())

					fileContent := string(fileContentByte)

					Expect(fileContent).To(ContainSubstring(checkContent))
				}
			}

			Expect(currentRequiredFileList).To(BeEmpty())
		}

		// Cleanup
		err = os.RemoveAll(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
	})

	It("succeed in retrieving ray cluster head logs", func() {
		expectedDirPath := "./raycluster-kuberay"
		expectedOutputStringFormat := `Command set to retrieve only head node logs\.\nNo output directory specified, creating dir under current directory using resource name\.\nDownloading log for Ray Node raycluster-kuberay-head-\w+`

		cmd := exec.Command("kubectl", "ray", "log", "--namespace", namespace, "raycluster-kuberay", "--node-type", "head")
		output, err := cmd.CombinedOutput()
		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(output))).Should(MatchRegexp(expectedOutputStringFormat))

		logDirInfo, err := os.Stat(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(logDirInfo.IsDir()).To(BeTrue())

		// Check the contents of the cluster directory
		fileList, err := os.ReadDir(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(fileList).To(HaveLen(1))

		for _, file := range fileList {
			Expect(file.IsDir()).To(BeTrue())

			// Check that the files exist and have correct content
			logList, err := os.ReadDir(path.Join(expectedDirPath, file.Name()))
			Expect(err).NotTo(HaveOccurred())

			currentRequiredFileList := make(map[string]string)

			maps.Copy(currentRequiredFileList, requiredFileSet)

			for _, logFile := range logList {
				if checkContent := currentRequiredFileList[logFile.Name()]; checkContent != "" {
					delete(currentRequiredFileList, logFile.Name())

					// read and check file content
					fileContentByte, err := os.ReadFile(path.Join(expectedDirPath, file.Name(), logFile.Name()))
					Expect(err).NotTo(HaveOccurred())

					fileContent := string(fileContentByte)

					Expect(fileContent).To(ContainSubstring(checkContent))
				}
			}

			Expect(currentRequiredFileList).To(BeEmpty())
		}

		// Cleanup
		err = os.RemoveAll(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
	})

	It("succeed in retrieving ray cluster worker logs", func() {
		expectedDirPath := "./raycluster-kuberay"
		expectedOutputStringFormat := `Command set to retrieve only worker node logs\.\nNo output directory specified, creating dir under current directory using resource name\.\nDownloading log for Ray Node raycluster-kuberay-workergroup-worker-\w+`

		cmd := exec.Command("kubectl", "ray", "log", "--namespace", namespace, "raycluster-kuberay", "--node-type", "worker")
		output, err := cmd.CombinedOutput()

		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(output))).Should(MatchRegexp(expectedOutputStringFormat))

		logDirInfo, err := os.Stat(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(logDirInfo.IsDir()).To(BeTrue())

		// Check the contents of the cluster directory
		fileList, err := os.ReadDir(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(fileList).To(HaveLen(1))

		for _, file := range fileList {
			Expect(file.IsDir()).To(BeTrue())

			// Check that the files exist and have correct content
			logList, err := os.ReadDir(path.Join(expectedDirPath, file.Name()))
			Expect(err).NotTo(HaveOccurred())

			currentRequiredFileList := make(map[string]string)

			maps.Copy(currentRequiredFileList, requiredFileSet)

			for _, logFile := range logList {
				if checkContent := currentRequiredFileList[logFile.Name()]; checkContent != "" {
					delete(currentRequiredFileList, logFile.Name())

					// read and check file content
					fileContentByte, err := os.ReadFile(path.Join(expectedDirPath, file.Name(), logFile.Name()))
					Expect(err).NotTo(HaveOccurred())

					fileContent := string(fileContentByte)

					Expect(fileContent).To(ContainSubstring(checkContent))
				}
			}

			Expect(currentRequiredFileList).To(BeEmpty())
		}

		// Cleanup
		err = os.RemoveAll(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
	})

	It("succeed in retrieving ray cluster logs within designated directory", func() {
		expectedDirPath := "./temporary-directory"
		expectedOutputStringFormat := `Command set to retrieve both head and worker node logs\.\nDownloading log for Ray Node raycluster-kuberay-head-\w+\nDownloading log for Ray Node raycluster-kuberay-workergroup-worker-\w+`

		err := os.MkdirAll(expectedDirPath, 0o755)
		Expect(err).NotTo(HaveOccurred())

		cmd := exec.Command("kubectl", "ray", "log", "--namespace", namespace, "raycluster-kuberay", "--node-type", "all", "--out-dir", expectedDirPath)
		output, err := cmd.CombinedOutput()

		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(output))).Should(MatchRegexp(expectedOutputStringFormat))

		// Check the contents of the cluster directory
		fileList, err := os.ReadDir(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
		Expect(fileList).To(HaveLen(2))

		// Cleanup
		err = os.RemoveAll(expectedDirPath)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should not succeed with non-existent cluster", func() {
		cmd := exec.Command("kubectl", "ray", "log", "--namespace", namespace, "fakeclustername")
		output, err := cmd.CombinedOutput()

		Expect(err).To(HaveOccurred())
		Expect(strings.TrimSpace(string(output))).To(ContainSubstring("No Ray nodes found for resource fakecluster"))
	})

	It("should not succeed with non-existent directory set", func() {
		cmd := exec.Command("kubectl", "ray", "log", "--namespace", namespace, "raycluster-kuberay", "--out-dir", "./fake-directory")
		output, err := cmd.CombinedOutput()

		Expect(err).To(HaveOccurred())
		Expect(strings.TrimSpace(string(output))).To(ContainSubstring("Directory does not exist."))
	})
})
