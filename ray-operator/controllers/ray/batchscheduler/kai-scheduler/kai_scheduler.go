package kaischeduler

// This KAI plugin relies on KAI-Scheduler's
// built-in PodGrouper to create PodGroups at
// runtime, so the plugin itself only needs to:
//   1. expose the scheduler name,
//   2. stamp pods with schedulerName + queue label.
// No PodGroup create/patch logic is included.

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/utils"
)

const (
	QueueLabelName = "kai.scheduler/queue"
)

type KaiScheduler struct{}

type KaiSchedulerFactory struct{}

func GetPluginName() string { return "kai-scheduler" }

func (k *KaiScheduler) Name() string { return GetPluginName() }

func (k *KaiScheduler) DoBatchSchedulingOnSubmission(_ context.Context, object metav1.Object) error {
	// In K8sJobMode, RayJob first creates a RayCluster,
	// and then creates the submitter pod after the RayCluster is ready.
	// KAI-Scheduler does not handle this two-phase creation pattern.
	// Other schedulers like Yunikorn and Volcano support this by pre-creating PodGroups.
	// For more details, see https://github.com/ray-project/kuberay/pull/4418#pullrequestreview-3751609041
	if rayJob, ok := object.(*rayv1.RayJob); ok {
		switch rayJob.Spec.SubmissionMode {
		case rayv1.K8sJobMode:
			return fmt.Errorf("KAI-Scheduler does not support RayJob with K8sJobMode: the submitter pod is created after RayCluster is ready, preventing proper gang scheduling")
		}
	}
	return nil
}

func (k *KaiScheduler) AddMetadataToChildResource(ctx context.Context, parent metav1.Object, child metav1.Object, _ string) {
	logger := ctrl.LoggerFrom(ctx).WithName("kai-scheduler")
	utils.AddSchedulerNameToObject(child, k.Name())

	parentLabel := parent.GetLabels()
	queue, ok := parentLabel[QueueLabelName]
	if !ok || queue == "" {
		logger.Info("Queue label missing from parent; child will remain pending",
			"requiredLabel", QueueLabelName)
		return
	}

	childLabels := child.GetLabels()
	if childLabels == nil {
		childLabels = make(map[string]string)
	}
	childLabels[QueueLabelName] = queue
	child.SetLabels(childLabels)
}

func (k *KaiScheduler) CleanupOnCompletion(_ context.Context, _ metav1.Object) (bool, error) {
	// KaiScheduler doesn't need cleanup
	return false, nil
}

func (kf *KaiSchedulerFactory) New(_ context.Context, _ *rest.Config, _ client.Client) (schedulerinterface.BatchScheduler, error) {
	return &KaiScheduler{}, nil
}

func (kf *KaiSchedulerFactory) AddToScheme(_ *runtime.Scheme) {
}

func (kf *KaiSchedulerFactory) ConfigureReconciler(b *builder.Builder) *builder.Builder {
	return b
}
