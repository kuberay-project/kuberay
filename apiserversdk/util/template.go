package util

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"

	"github.com/ray-project/kuberay/apiserver/pkg/manager"
	"github.com/ray-project/kuberay/apiserver/pkg/model"
	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
)

// compute_template_middleware.go
func NewComputeTemplateMiddleware(clientManager manager.ClientManagerInterface) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			namespace := r.PathValue("namespace")

			// Read request body
			bodyBytes, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "Failed to read request body", http.StatusBadRequest)
				return
			}
			defer r.Body.Close()

			// Convert request body to Golang Map object
			contentType := r.Header.Get("Content-Type")
			requestMap, err := convertRequestBodyToMap(bodyBytes, contentType)
			if err != nil {
				klog.Errorf("Failed to convert request body to map: %v", err)
				http.Error(w, "Failed to convert request body to Golang map object", http.StatusBadRequest)
				return
			}
			spec, ok := requestMap["spec"].(map[string]any)
			if !ok {
				klog.Infof("ComputeTemplate middleware: spec is not a map, skipping compute template processing")
				// Continue with original request body without compute template processing
				r.Body = io.NopCloser(bytes.NewReader(bodyBytes))
				next.ServeHTTP(w, r)
				return
			}

			// Process compute templates and apply them to the request
			var headGroupMap map[string]any
			var workerGroupMaps []any
			if rayClusterSpec, ok := spec["rayClusterSpec"].(map[string]any); ok {
				// For RayJob, get from spec.rayClusterSpec.headGroupSpec and spec.rayClusterSpec.workerGroupSpecs
				if headGroup, ok := rayClusterSpec["headGroupSpec"].(map[string]any); ok {
					headGroupMap = headGroup
				}
				if workerGroups, ok := rayClusterSpec["workerGroupSpecs"].([]any); ok {
					workerGroupMaps = workerGroups
				}
			} else if rayClusterConfig, ok := spec["rayClusterConfig"].(map[string]any); ok {
				// For RayService, get from spec.rayClusterConfig.headGroupSpec and spec.rayClusterConfig.workerGroupSpecs
				if headGroup, ok := rayClusterConfig["headGroupSpec"].(map[string]any); ok {
					headGroupMap = headGroup
				}
				if workerGroups, ok := rayClusterConfig["workerGroupSpecs"].([]any); ok {
					workerGroupMaps = workerGroups
				}
			} else {
				// For RayCluster, get from spec.headGroupSpec and spec.workerGroupSpecs
				if headGroup, ok := spec["headGroupSpec"].(map[string]any); ok {
					headGroupMap = headGroup
				}
				if workerGroups, ok := spec["workerGroupSpecs"].([]any); ok {
					workerGroupMaps = workerGroups
				}
			}

			resourceManager := manager.NewResourceManager(clientManager)

			if headGroupMap != nil {
				computeTemplate, err := getComputeTemplate(context.Background(), resourceManager, headGroupMap, namespace)
				if err != nil {
					klog.Errorf("ComputeTemplate middleware: Failed to get compute template for head group: %v", err)
					http.Error(w, err.Error(), http.StatusUnprocessableEntity)
					return
				}
				if computeTemplate != nil {
					applyComputeTemplateToRequest(computeTemplate, &headGroupMap, "head")
				}
			}

			// Apply compute templates to worker groups
			for i, workerGroupSpec := range workerGroupMaps {
				if workerGroupMap, ok := workerGroupSpec.(map[string]any); ok {
					computeTemplate, err := getComputeTemplate(context.Background(), resourceManager, workerGroupMap, namespace)
					if err != nil {
						klog.Errorf("ComputeTemplate middleware: Failed to get compute template for worker group %d: %v", i, err)
						http.Error(w, err.Error(), http.StatusUnprocessableEntity)
						return
					}
					if computeTemplate != nil {
						klog.Infof("ComputeTemplate middleware: Applying compute template %s to worker group %d", computeTemplate.Name, i)
						applyComputeTemplateToRequest(computeTemplate, &workerGroupMap, "worker")
					}
				}
			}

			// Convert the modified requestMap to JSON since K8s API expects JSON format
			jsonBytes, err := convertMapToJSON(requestMap)
			if err != nil {
				klog.Errorf("ComputeTemplate middleware: Failed to convert to JSON: %v", err)
				http.Error(w, "Failed to process request", http.StatusInternalServerError)
				return
			}

			klog.Infof("ComputeTemplate middleware: Successfully processed request, sending to next handler")
			// Update Content-Type to application/json and Content-Length header to match the new body size
			r.Header.Set("Content-Type", "application/json")
			r.ContentLength = int64(len(jsonBytes))
			r.Header.Set("Content-Length", fmt.Sprintf("%d", len(jsonBytes)))
			r.Body = io.NopCloser(bytes.NewReader(jsonBytes))

			next.ServeHTTP(w, r)
		})
	}
}

// Convert the request body to map, handling both JSON and YAML formats
func convertRequestBodyToMap(requestBody []byte, contentType string) (map[string]any, error) {
	var requestMap map[string]any

	// Check content type to determine format
	if strings.Contains(contentType, "application/json") {
		if err := json.Unmarshal(requestBody, &requestMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
		}
	} else if strings.Contains(contentType, "application/yaml") {
		if err := yaml.Unmarshal(requestBody, &requestMap); err != nil {
			return nil, fmt.Errorf("failed to unmarshal YAML: %w", err)
		}
	} else {
		return nil, fmt.Errorf("Cannot unmarshal content type that's not JSON or YAML: %s", contentType)
	}

	return requestMap, nil
}

// Convert YAML request map to JSON bytes for K8s API
func convertMapToJSON(requestMap map[string]any) ([]byte, error) {
	jsonBytes, err := json.Marshal(requestMap)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request map to JSON: %w", err)
	}
	return jsonBytes, nil
}

// Get the compute template by extracting the name from request and query the compute template
func getComputeTemplate(ctx context.Context, resourceManager *manager.ResourceManager, clusterSpecMap map[string]any, nameSpace string) (*api.ComputeTemplate, error) {
	name, ok := clusterSpecMap["computeTemplate"].(string)
	if !ok {
		// No compute template name found, directly return
		klog.Infof("ComputeTemplate middleware: No computeTemplate field found in spec")
		return nil, nil
	}

	configMap, err := resourceManager.GetComputeTemplate(ctx, name, nameSpace)
	if err != nil {
		return nil, fmt.Errorf("Cannot get compute template for name '%s' in namespace '%s', error: %w", name, nameSpace, err)
	}
	computeTemplate := model.FromKubeToAPIComputeTemplate(configMap)

	return computeTemplate, nil
}

// Apply the computeTemplate into the clusterSpec map. The clusterSpec map is the map representation
// for headGroupSpec or workerGroupSpec
func applyComputeTemplateToRequest(computeTemplate *api.ComputeTemplate, clusterSpecMap *map[string]any, group string) {
	// calculate resources
	cpu := fmt.Sprint(computeTemplate.GetCpu())
	memoryUnit := computeTemplate.GetMemoryUnit()
	memory := fmt.Sprintf("%d%s", computeTemplate.GetMemory(), memoryUnit)

	if template, ok := (*clusterSpecMap)["template"].(map[string]any); ok {
		// Add compute template name to annotation

		metadata, ok := template["metadata"].(map[string]any)
		if !ok {
			metadata = make(map[string]any)
			template["metadata"] = metadata
		}
		annotations, ok := metadata["annotations"].(map[string]any)
		if !ok {
			annotations = make(map[string]any)
			metadata["annotations"] = annotations
		}
		annotations[util.RayClusterComputeTemplateAnnotationKey] = computeTemplate.Name

		// apply resources to containers
		if spec, ok := template["spec"].(map[string]any); ok {
			if containers, ok := spec["containers"].([]any); ok {
				for _, container := range containers {
					if containerMap, ok := container.(map[string]any); ok {
						// Get or create resources section for this container
						resources, exists := containerMap["resources"].(map[string]any)
						if !exists {
							resources = make(map[string]any)
							containerMap["resources"] = resources
						}

						// Set limits
						limits, exists := resources["limits"].(map[string]any)
						if !exists {
							limits = make(map[string]any)
							resources["limits"] = limits
						}
						limits["cpu"] = cpu
						limits["memory"] = memory

						// Set requests
						requests, exists := resources["requests"].(map[string]any)
						if !exists {
							requests = make(map[string]any)
							resources["requests"] = requests
						}
						requests["cpu"] = cpu
						requests["memory"] = memory

						// Only apply followings if container name is "ray-head" for head group or "ray-worker"
						// for worker group
						if containerMap["name"] == fmt.Sprintf("ray-%s", group) {
							if gpu := computeTemplate.GetGpu(); gpu != 0 {
								accelerator := "nvidia.com/gpu"
								if len(computeTemplate.GetGpuAccelerator()) != 0 {
									accelerator = computeTemplate.GetGpuAccelerator()
								}
								limits[accelerator] = gpu
								requests[accelerator] = gpu
							}

							for k, v := range computeTemplate.GetExtendedResources() {
								limits[k] = v
								requests[k] = v
							}

						}
					}
				}
			}

			if computeTemplate.Tolerations != nil {
				// Get existing tolerations
				var tolerations []any
				if existingTolerations, exists := spec["tolerations"].([]any); exists {
					tolerations = existingTolerations
				}

				// Add new tolerations from compute template
				for _, t := range computeTemplate.Tolerations {
					toleration := map[string]any{
						"key":      t.Key,
						"operator": t.Operator,
						"value":    t.Value,
						"effect":   t.Effect,
					}
					tolerations = append(tolerations, toleration)
				}

				spec["tolerations"] = tolerations
			}
		}
	}
}
