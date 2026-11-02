package utils

import (
	"regexp"
	"strings"
)

// draImplicitExtendedResourcePrefix is the prefix of the extended resource name that Kubernetes
// derives for every DRA DeviceClass (KEP-5004): "deviceclass.resource.kubernetes.io/<DeviceClass name>".
// A Pod can request devices of that class under this name in resources.limits without writing a
// ResourceClaim. GPU DRA drivers do not set DeviceClass.spec.extendedResourceName by default, so this
// implicit name is what their users write.
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
	// DRA implicit extended resource name like "deviceclass.resource.kubernetes.io/gpu.nvidia.com".
	// The known GPU DRA drivers name their DeviceClass "gpu.<vendor domain>" (gpu.nvidia.com, gpu.amd.com,
	// gpu.intel.com, gpu.example.com), so only that shape is treated as a GPU. A DeviceClass named plainly
	// "gpu" is already covered by the suffix check above. DRA MIG classes such as "mig.nvidia.com" are not
	// recognised here (the device-plugin MIG names matched by the regexp above still are); whether one
	// DRA MIG slice should count as one Ray GPU is a separate decision.
	if className, ok := strings.CutPrefix(key, draImplicitExtendedResourcePrefix); ok {
		return strings.HasPrefix(className, "gpu.")
	}
	return false
}
