package v1

import (
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
	"github.com/ray-project/kuberay/ray-operator/pkg/features"
)

// validateLabelRefs checks the labelRefs of every worker group in spec against the operator allowlist.
// old is the spec before an update, nil on create. A group whose labelRefs are unchanged skips the gate and allowlist
// checks so disabling the gate or narrowing the allowlist does not block unrelated updates such as finalizer removal.
// annotations are the admitted object's metadata annotations, base is the field path of spec in that object
func validateLabelRefs(spec, old *rayv1.RayClusterSpec, annotations map[string]string, allowed []string, base *field.Path) *field.Error {
	for i := range spec.WorkerGroupSpecs {
		group := &spec.WorkerGroupSpecs[i]
		if len(group.LabelRefs) == 0 {
			continue
		}
		path := base.Child("workerGroupSpecs").Index(i).Child("labelRefs")
		unchanged := false
		if old != nil {
			if oldGroup := findWorkerGroupSpec(old, group.GroupName); oldGroup != nil {
				unchanged = slices.Equal(oldGroup.LabelRefs, group.LabelRefs)
			}
		}
		if !unchanged && !features.Enabled(features.NodeLabelDelivery) {
			return field.Forbidden(path, "requires the NodeLabelDelivery feature gate to be enabled")
		}

		// node label delivery rewrites the KubeRay-generated ray start command, so the user must not replace it.
		// the annotation is read from the CR metadata and copied onto the pod template, so check both
		if overwritesContainerCmd(annotations) || overwritesContainerCmd(group.Template.Annotations) {
			return field.Forbidden(path, fmt.Sprintf("cannot be combined with the %s annotation; node label delivery needs the KubeRay-generated ray start command", utils.RayOverwriteContainerCmdAnnotationKey))
		}
		if len(group.Template.Spec.Containers) > utils.RayContainerIndex {
			container := group.Template.Spec.Containers[utils.RayContainerIndex]
			if strings.Contains(strings.Join(container.Command, " ")+" "+strings.Join(container.Args, " "), "ray start") {
				return field.Forbidden(path, "cannot be combined with a container command that runs ray start; node label delivery needs the KubeRay-generated ray start command")
			}
			if utils.EnvVarExists(utils.RAY_NODE_LABELS_JSON, container.Env) {
				return field.Forbidden(path, fmt.Sprintf("cannot be combined with the %s env var in the pod template; it is managed by KubeRay", utils.RAY_NODE_LABELS_JSON))
			}
		}
		// node label delivery passes --labels-file itself, and --labels would override the delivered keys
		for _, param := range []string{"labels", "labels-file"} {
			if _, ok := group.RayStartParams[param]; ok {
				return field.Forbidden(path, fmt.Sprintf("cannot be combined with rayStartParams %s", param))
			}
		}
		// check for duplicate label keys
		seen := make(map[string]bool, len(group.LabelRefs))
		for j, ref := range group.LabelRefs {
			refPath := path.Index(j)
			fieldPath := ref.ValueFrom.NodeRef.FieldPath
			nodeLabel, err := utils.NodeLabelKey(fieldPath)
			if err != nil {
				return field.Invalid(refPath.Child("valueFrom", "nodeRef", "fieldPath"), fieldPath, err.Error())
			}
			if errs := validation.IsQualifiedName(nodeLabel); len(errs) > 0 {
				return field.Invalid(refPath.Child("valueFrom", "nodeRef", "fieldPath"), nodeLabel, strings.Join(errs, "; "))
			}
			if !unchanged && !slices.Contains(allowed, nodeLabel) {
				return field.Forbidden(refPath.Child("valueFrom", "nodeRef", "fieldPath"), fmt.Sprintf("node label %q is not in the operator's allowedNodeLabels", nodeLabel))
			}
			// name defaults to the node label key
			rayKey := ref.Name
			if rayKey == "" {
				rayKey = nodeLabel
			}
			if errs := validation.IsQualifiedName(rayKey); len(errs) > 0 {
				return field.Invalid(refPath.Child("name"), rayKey, strings.Join(errs, "; "))
			}
			if seen[rayKey] {
				return field.Duplicate(refPath, rayKey)
			}
			if _, ok := group.Labels[rayKey]; ok {
				return field.Forbidden(refPath, fmt.Sprintf("Ray label %q is also set in labels; the static value would override the delivered one", rayKey))
			}
			seen[rayKey] = true
		}
	}
	return nil
}

func overwritesContainerCmd(annotations map[string]string) bool {
	v, ok := annotations[utils.RayOverwriteContainerCmdAnnotationKey]
	return ok && strings.ToLower(v) == "true"
}
