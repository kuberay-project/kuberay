package e2e

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestKubectlRayCommand(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Kubectl Ray e2e Test Suite")
}
