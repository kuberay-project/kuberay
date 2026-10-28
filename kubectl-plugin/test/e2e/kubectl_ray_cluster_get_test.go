package e2e

import (
	"bytes"
	"os/exec"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/printers"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
)

var _ = Describe("Calling ray plugin `get` command", func() {
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
		cmd := exec.Command("kubectl", "ray", "get", "cluster", "--namespace", namespace)
		output, err := cmd.CombinedOutput()

		expectedOutputTablePrinter := printers.NewTablePrinter(printers.PrintOptions{})
		expectedTestResultTable := &v1.Table{
			ColumnDefinitions: []v1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Namespace", Type: "string"},
				{Name: "Desired Workers", Type: "string"},
				{Name: "Available Workers", Type: "string"},
				{Name: "CPUs", Type: "string"},
				{Name: "GPUs", Type: "string"},
				{Name: "TPUs", Type: "string"},
				{Name: "Memory", Type: "string"},
				{Name: "Condition", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Age", Type: "string"},
			},
		}

		expectedTestResultTable.Rows = append(expectedTestResultTable.Rows, v1.TableRow{
			Cells: []any{
				"raycluster-kuberay",
				namespace,
				"1",
				"1",
				"2",
				"0",
				"0",
				"6Gi",
				rayv1.RayClusterProvisioned,
				rayv1.Ready,
			},
		})

		var resbuffer bytes.Buffer
		bufferr := expectedOutputTablePrinter.PrintObj(expectedTestResultTable, &resbuffer)
		Expect(bufferr).NotTo(HaveOccurred())

		Expect(err).NotTo(HaveOccurred())
		Expect(strings.TrimSpace(string(output))).To(ContainSubstring(strings.TrimSpace(resbuffer.String())))
	})

	It("should not succeed", func() {
		cmd := exec.Command("kubectl", "ray", "get", "cluster", "--namespace", namespace, "fakeclustername", "anotherfakeclustername")
		output, err := cmd.CombinedOutput()

		Expect(err).To(HaveOccurred())
		Expect(output).ToNot(ContainElements("fakeclustername"))
	})
})
