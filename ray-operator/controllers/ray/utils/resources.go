package utils

import (
	"regexp"
	"strings"
)

// Prefix of the implicit extended resource name Kubernetes derives for every DRA DeviceClass (KEP-5004).
const draImplicitExtendedResourcePrefix = "deviceclass.resource.kubernetes.io/"

var nvidiaMIGResourceKeyRegexp = regexp.MustCompile(`nvidia\.com/mig-\d+g\.\d+gb$`)

func IsGPUResourceKey(key string) bool {
	// ending with "gpu" like "nvidia.com/gpu"
	if strings.HasSuffix(key, "gpu") {
		return true
	}
	// Nvidia Multi-Instance GPU in the form of "nvidia.com/mig-<slice_count>g.<memory_size>gb" like "nvidia.com/mig-2g.32gb"
	// reference: https://github.com/NVIDIA/k8s-device-plugin#configuration-option-details
	if nvidiaMIGResourceKeyRegexp.MatchString(key) {
		return true
	}
	// DRA implicit extended resource name of a "gpu.<vendor>" DeviceClass like "deviceclass.resource.kubernetes.io/gpu.nvidia.com"
	if className, ok := strings.CutPrefix(key, draImplicitExtendedResourcePrefix); ok {
		return strings.HasPrefix(className, "gpu.")
	}
	return false
}
