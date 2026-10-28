package strconv

import (
	"testing"
	"unsafe"

	"github.com/onsi/gomega"

	utils "github.com/ray-project/kuberay/ray-operator/pkg/utils"
)

func TestStringConversion(t *testing.T) {
	g := gomega.NewWithT(t)

	str := "hello world"

	// Test string to byte array conversion.
	arr := utils.ConvertStringToByteSlice(str)
	g.Expect(arr).Should(gomega.Equal([]byte(str)))
	g.Expect(&arr[0]).Should(gomega.Equal(unsafe.StringData(str)))

	// Test byte array to string conversion.
	convStr := utils.ConvertByteSliceToString(arr)
	g.Expect(str).Should(gomega.Equal(convStr))
	g.Expect(unsafe.StringData(convStr)).Should(gomega.Equal(&arr[0]))
}
