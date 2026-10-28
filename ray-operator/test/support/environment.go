package support

import (
	"os"
)

const (
	// KuberayTestOutputDir is the testing output directory, to write output files into.
	KuberayTestOutputDir = "KUBERAY_TEST_OUTPUT_DIR"

	// KuberayTestRayVersion is the version of Ray to use for testing.
	KuberayTestRayVersion = "KUBERAY_TEST_RAY_VERSION"

	// KuberayTestRayImage is the Ray image to use for testing.
	KuberayTestRayImage = "KUBERAY_TEST_RAY_IMAGE"

	KuberayTestUpgradeImage = "KUBERAY_TEST_UPGRADE_IMAGE"
)

func GetRayVersion() string {
	return lookupEnvOrDefault(KuberayTestRayVersion, RayVersion)
}

func GetRayImage() string {
	return lookupEnvOrDefault(KuberayTestRayImage, RayImage)
}

func GetKubeRayUpgradeVersion() string {
	return lookupEnvOrDefault(KuberayTestUpgradeImage, KuberayUpgradeVersion)
}

func lookupEnvOrDefault(key, value string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return value
}
