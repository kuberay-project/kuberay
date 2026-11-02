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

var rayServiceLog = logf.Log.WithName("rayservice-resource")

// SetupRayServiceWebhookWithManager registers the RayService webhook, configured from the operator config
func SetupRayServiceWebhookWithManager(mgr ctrl.Manager, config configapi.Configuration) error {
	return ctrl.NewWebhookManagedBy(mgr, &rayv1.RayService{}).
		WithValidator(&RayServiceWebhook{AllowedNodeLabels: config.AllowedNodeLabels}).
		Complete()
}

type RayServiceWebhook struct {
	// AllowedNodeLabels is the operator allowlist for labelRefs
	AllowedNodeLabels []string
}

//+kubebuilder:webhook:path=/validate-ray-io-v1-rayservice,mutating=false,failurePolicy=fail,sideEffects=None,groups=ray.io,resources=rayservices,verbs=create;update,versions=v1,name=vrayservice.kb.io,admissionReviewVersions=v1

var _ admission.Validator[*rayv1.RayService] = &RayServiceWebhook{}

// ValidateCreate implements admission.Validator so a webhook will be registered for the type
func (w *RayServiceWebhook) ValidateCreate(_ context.Context, rayService *rayv1.RayService) (admission.Warnings, error) {
	rayServiceLog.Info("validate create", "name", rayService.Name)
	return nil, w.validateRayService(nil, rayService)
}

// ValidateUpdate implements admission.Validator so a webhook will be registered for the type
func (w *RayServiceWebhook) ValidateUpdate(_ context.Context, old, rayService *rayv1.RayService) (admission.Warnings, error) {
	rayServiceLog.Info("validate update", "name", rayService.Name)
	return nil, w.validateRayService(&old.Spec.RayClusterSpec, rayService)
}

// ValidateDelete implements admission.Validator so a webhook will be registered for the type
func (w *RayServiceWebhook) ValidateDelete(_ context.Context, _ *rayv1.RayService) (admission.Warnings, error) {
	return nil, nil
}

func (w *RayServiceWebhook) validateRayService(old *rayv1.RayClusterSpec, rayService *rayv1.RayService) error {
	var allErrs field.ErrorList

	if err := utils.ValidateRayServiceMetadata(rayService.ObjectMeta); err != nil {
		allErrs = append(allErrs, field.Invalid(field.NewPath("metadata").Child("name"), rayService.Name, err.Error()))
	}

	if err := validateLabelRefs(&rayService.Spec.RayClusterSpec, old, rayService.Annotations, w.AllowedNodeLabels, field.NewPath("spec").Child("rayClusterConfig")); err != nil {
		allErrs = append(allErrs, err)
	}

	if len(allErrs) == 0 {
		return nil
	}

	return apierrors.NewInvalid(
		schema.GroupKind{Group: "ray.io", Kind: "RayService"},
		rayService.Name, allErrs)
}
