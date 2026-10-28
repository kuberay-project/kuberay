package dashboardclient

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestDashboardClient(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Dashboard Client Suite")
}
