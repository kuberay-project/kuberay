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

var rayJobLog = logf.Log.WithName("rayjob-resource")

// SetupRayJobWebhookWithManager registers the RayJob webhook, configured from the operator config
func SetupRayJobWebhookWithManager(mgr ctrl.Manager, config configapi.Configuration) error {
	return ctrl.NewWebhookManagedBy(mgr, &rayv1.RayJob{}).
		WithValidator(&RayJobWebhook{AllowedNodeLabels: config.AllowedNodeLabels}).
		Complete()
}

type RayJobWebhook struct {
	// AllowedNodeLabels is the operator allowlist for labelRefs
	AllowedNodeLabels []string
}

//+kubebuilder:webhook:path=/validate-ray-io-v1-rayjob,mutating=false,failurePolicy=fail,sideEffects=None,groups=ray.io,resources=rayjobs,verbs=create;update,versions=v1,name=vrayjob.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*rayv1.RayJob] = &RayJobWebhook{}

// ValidateCreate implements admission.Validator so a webhook will be registered for the type
func (w *RayJobWebhook) ValidateCreate(_ context.Context, rayJob *rayv1.RayJob) (admission.Warnings, error) {
	rayJobLog.Info("validate create", "name", rayJob.Name)
	return nil, w.validateRayJob(nil, rayJob)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (w *RayJobWebhook) ValidateUpdate(_ context.Context, old, rayJob *rayv1.RayJob) (admission.Warnings, error) {
	rayJobLog.Info("validate update", "name", rayJob.Name)
	return nil, w.validateRayJob(old.Spec.RayClusterSpec, rayJob)
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (w *RayJobWebhook) ValidateDelete(_ context.Context, _ *rayv1.RayJob) (admission.Warnings, error) {
	return nil, nil
}

func (w *RayJobWebhook) validateRayJob(old *rayv1.RayClusterSpec, rayJob *rayv1.RayJob) error {
	var allErrs field.ErrorList

	if err := utils.ValidateRayJobMetadata(rayJob.ObjectMeta); err != nil {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata").Child("name"), rayJob.Name, err.Error()))
	}

	if rayJob.Spec.RayClusterSpec != nil {
		if err := validateLabelRefs(rayJob.Spec.RayClusterSpec, old, rayJob.Annotations, w.AllowedNodeLabels, field.NewPath("spec").Child("rayClusterSpec")); err != nil {
			allErrs = append(allErrs, err)
		}
	}

	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(
		schema.GroupKind{Group: "ray.io", Kind: "RayJob"},
		rayJob.Name, allErrs)
}
