package yunikorn

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	batchschedulerutils "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/utils"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	SchedulerName                       string = "yunikorn"
	YuniKornPodApplicationIDLabelName   string = "applicationId"
	YuniKornPodQueueLabelName           string = "queue"
	RayApplicationIDLabelName           string = "yunikorn.apache.org/app-id"
	RayApplicationQueueLabelName        string = "yunikorn.apache.org/queue"
	YuniKornTaskGroupNameAnnotationName string = "yunikorn.apache.org/task-group-name"
	YuniKornTaskGroupsAnnotationName    string = "yunikorn.apache.org/task-groups"
)

type YuniKornScheduler struct{}

type YuniKornSchedulerFactory struct{}

func GetPluginName() string {
	return SchedulerName
}

func (y *YuniKornScheduler) Name() string {
	return GetPluginName()
}

func (y *YuniKornScheduler) DoBatchSchedulingOnSubmission(_ context.Context, _ metav1.Object) error {
	// yunikorn doesn't require any resources to be created upfront
	// this is a no-opt for this implementation
	return nil
}

// propagateTaskGroupsAnnotation is a helper function that propagates the task groups annotation to the child
// if the parent has the task groups annotation, it will be copied to the child
// if the parent doesn't have the task groups annotation, a new one will be created
// TODO: remove the legacy labels, i.e "applicationId" and "queue", directly populate labels
// RayApplicationIDLabelName and RayApplicationQueueLabelName to pod labels.
// Currently we use this function to translate labels "yunikorn.apache.org/app-id" and "yunikorn.apache.org/queue"
// to legacy labels "applicationId" and "queue", this is for the better compatibilities to support older yunikorn
// versions.
func propagateTaskGroupsAnnotation(parent metav1.Object, child metav1.Object) error {
	var taskGroupsAnnotationValue string
	if parentAnnotations, exist := parent.GetAnnotations()[YuniKornTaskGroupsAnnotationName]; exist && parentAnnotations != "" {
		taskGroupsAnnotationValue = parentAnnotations
	}
	if taskGroupsAnnotationValue == "" {
		var err error
		taskGroupsAnnotationValue, err = getTaskGroupsAnnotationValue(parent)
		if err != nil {
			return err
		}
	}
	annotations := child.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[YuniKornTaskGroupsAnnotationName] = taskGroupsAnnotationValue
	child.SetAnnotations(annotations)
	return nil
}

func populateLabelsFromObject(parent metav1.Object, child metav1.Object, sourceKey string, targetKey string) {
	labels := child.GetLabels()
	if labels == nil {
		labels = make(map[string]string)
	}
	if parentLabel, exist := parent.GetLabels()[sourceKey]; exist && parentLabel != "" {
		labels[targetKey] = parentLabel
	}
	child.SetLabels(labels)
}

func getTaskGroupsAnnotationValue(obj metav1.Object) (string, error) {
	taskGroups := newTaskGroups()
	switch obj := obj.(type) {
	case *rayv1.RayCluster:
		taskGroups = newTaskGroupsFromRayClusterSpec(&obj.Spec)
	case *rayv1.RayJob:
		taskGroups = newTaskGroupsFromRayJobSpec(&obj.Spec)
	}
	taskGroupsAnnotationValue, err := taskGroups.marshal()
	if err != nil {
		return "", err
	}
	return taskGroupsAnnotationValue, nil
}

func addTaskGroupNameAnnotation(obj metav1.Object, groupName string) {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[YuniKornTaskGroupNameAnnotationName] = groupName
	obj.SetAnnotations(annotations)
}

func (y *YuniKornScheduler) isGangSchedulingEnabled(obj metav1.Object) bool {
	_, exist := obj.GetLabels()[utils.RayGangSchedulingEnabled]
	return exist
}

func (y *YuniKornScheduler) AddMetadataToChildResource(ctx context.Context, parent metav1.Object, child metav1.Object, groupName string) {
	logger := ctrl.LoggerFrom(ctx).WithName(SchedulerName)

	populateLabelsFromObject(parent, child, RayApplicationIDLabelName, YuniKornPodApplicationIDLabelName)
	populateLabelsFromObject(parent, child, RayApplicationQueueLabelName, YuniKornPodQueueLabelName)
	batchschedulerutils.AddSchedulerNameToObject(child, y.Name())

	if y.isGangSchedulingEnabled(parent) {
		logger.Info("gang scheduling is enabled, propagating task groups annotation to child", "name", child.GetName(), "namespace", child.GetNamespace())

		err := propagateTaskGroupsAnnotation(parent, child)
		if err != nil {
			logger.Error(err, "failed to add gang scheduling related annotations to object, "+
				"gang scheduling will not be enabled for this workload",
				"name", child.GetName(), "namespace", child.GetNamespace())
			return
		}
		if groupName != "" {
			addTaskGroupNameAnnotation(child, groupName)
		}
	}
}

func (y *YuniKornScheduler) CleanupOnCompletion(_ context.Context, _ metav1.Object) (bool, error) {
	// YuniKorn doesn't need cleanup
	return false, nil
}

func (yf *YuniKornSchedulerFactory) New(_ context.Context, _ *rest.Config, _ client.Client) (schedulerinterface.BatchScheduler, error) {
	return &YuniKornScheduler{}, nil
}

func (yf *YuniKornSchedulerFactory) AddToScheme(_ *runtime.Scheme) {
	// No extra scheme needs to be registered
}

func (yf *YuniKornSchedulerFactory) ConfigureReconciler(b *builder.Builder) *builder.Builder {
	return b
}
