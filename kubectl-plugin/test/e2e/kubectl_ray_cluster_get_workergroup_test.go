package e2e

import (
	"bytes"
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/printers"
)

var _ = Describe("Calling ray plugin `get workergroups` command to list worker group status", func() {
	var namespace string

	BeforeEach(func() {
		namespace = createTestNamespace()
		deployTestRayCluster(namespace)
		DeferCleanup(func() {
			deleteTestNamespace(namespace)
			namespace = ""
		})
	})

	It("succeed in getting ray cluster information", func() {
		cmd := exec.Command("kubectl", "ray", "get", "workergroups", "--namespace", namespace)
		output, err := cmd.CombinedOutput()

		expectedOutputTablePrinter := printers.NewTablePrinter(printers.PrintOptions{})
		expectedTestResultTable := &v1.Table{
			ColumnDefinitions: []v1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Min", Type: "string"},
				{Name: "Max", Type: "string"},
				{Name: "Replicas", Type: "string"},
				{Name: "CPUs", Type: "string"},
				{Name: "GPUs", Type: "string"},
				{Name: "TPUs", Type: "string"},
				{Name: "Memory", Type: "string"},
				{Name: "Cluster", Type: "string"},
			},
		}

		expectedTestResultTable.Rows = append(expectedTestResultTable.Rows, v1.TableRow{
			Cells: []any{
				"workergroup",
				"1",
				"5",
				"1/1",
				"1",
				"0",
				"0",
				"1Gi",
				"raycluster-kuberay",
			},
		})

		var resbuffer bytes.Buffer
		bufferr := expectedOutputTablePrinter.PrintObj(expectedTestResultTable, &resbuffer)
		Expect(bufferr).NotTo(HaveOccurred())

		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(output))).To(ContainSubstring(strings.TrimSpace(resbuffer.String())))
	})

	It("should not succeed", func() {
		cmd := exec.Command("kubectl", "ray", "get", "workergroups", "--namespace", namespace, "fakeWorkerGroupsName")
		output, err := cmd.CombinedOutput()

		Expect(err).To(HaveOccurred())
		Expect(string(output)).To(ContainSubstring("not found"))
	})
})
