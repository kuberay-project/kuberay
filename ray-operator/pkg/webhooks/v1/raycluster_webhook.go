package v1

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	configapi "github.com/ray-project/kuberay/ray-operator/apis/config/v1alpha1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

var rayClusterLog = logf.Log.WithName("raycluster-resource")

// SetupRayClusterWebhookWithManager registers the RayCluster webhook, configured from the operator config
func SetupRayClusterWebhookWithManager(mgr ctrl.Manager, config configapi.Configuration) error {
	return ctrl.NewWebhookManagedBy(mgr, &rayv1.RayCluster{}).
		WithValidator(&RayClusterWebhook{AllowedNodeLabels: config.AllowedNodeLabels}).
		Complete()
}

type RayClusterWebhook struct {
	// AllowedNodeLabels is the operator allowlist for labelRefs
	AllowedNodeLabels []string
}

//+kubebuilder:webhook:path=/validate-ray-io-v1-raycluster,mutating=false,failurePolicy=fail,sideEffects=None,groups=ray.io,resources=rayclusters,verbs=create;update,versions=v1,name=vraycluster.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*rayv1.RayCluster] = &RayClusterWebhook{}

// ValidateCreate implements admission.Validator so a webhook will be registered for the type
func (w *RayClusterWebhook) ValidateCreate(_ context.Context, rayCluster *rayv1.RayCluster) (admission.Warnings, error) {
	rayClusterLog.Info("validate create", "name", rayCluster.Name)
	return nil, w.validateRayCluster(nil, rayCluster)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (w *RayClusterWebhook) ValidateUpdate(_ context.Context, old, rayCluster *rayv1.RayCluster) (admission.Warnings, error) {
	rayClusterLog.Info("validate update", "name", rayCluster.Name)
	return nil, w.validateRayCluster(&old.Spec, rayCluster)
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (w *RayClusterWebhook) ValidateDelete(_ context.Context, _ *rayv1.RayCluster) (admission.Warnings, error) {
	return nil, nil
}

func (w *RayClusterWebhook) validateRayCluster(old *rayv1.RayClusterSpec, rayCluster *rayv1.RayCluster) error {
	var allErrs field.ErrorList

	if err := utils.ValidateRayClusterMetadata(rayCluster.ObjectMeta); err != nil {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata").Child("name"), rayCluster.Name, err.Error()))
	}

	if err := w.validateWorkerGroups(rayCluster); err != nil {
		allErrs = append(allErrs, err)
	}

	if err := validateLabelRefs(&rayCluster.Spec, old, rayCluster.Annotations, w.AllowedNodeLabels, field.NewPath("spec")); err != nil {
		allErrs = append(allErrs, err)
	}

	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(
		schema.GroupKind{Group: "ray.io", Kind: "RayCluster"},
		rayCluster.Name, allErrs)
}

func (w *RayClusterWebhook) validateWorkerGroups(rayCluster *rayv1.RayCluster) *field.Error {
	workerGroupNames := make(map[string]bool)

	for i, workerGroup := range rayCluster.Spec.WorkerGroupSpecs {
		if _, ok := workerGroupNames[workerGroup.GroupName]; ok {
			return field.Invalid(field.NewPath("spec").Child("workerGroupSpecs").Index(i), workerGroup, "worker group names must be unique")
		}
		workerGroupNames[workerGroup.GroupName] = true
	}

	return nil
}
