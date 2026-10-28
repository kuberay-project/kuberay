// Package v1alpha1 contains API Schema definitions for the ray v1alpha1 API group
//
// Deprecated: ray.io/v1alpha1 is served for backward compatibility only, is
// frozen at its December 2023 feature set, and will be unavailable in KubeRay
// 1.9. Use github.com/ray-project/kuberay/ray-operator/apis/ray/v1 instead.
// +kubebuilder:object:generate=true
// +groupName=ray.io
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// GroupVersion is group version used to register these objects
	GroupVersion = schema.GroupVersion{Group: "ray.io", Version: "v1alpha1"}

	// SchemeBuilder is used to add go types to the GroupVersionKind scheme
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme adds the types in this group-version to the given scheme.
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion,
		&RayCluster{}, &RayClusterList{},
		&RayJob{}, &RayJobList{},
		&RayService{}, &RayServiceList{},
	)
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}
