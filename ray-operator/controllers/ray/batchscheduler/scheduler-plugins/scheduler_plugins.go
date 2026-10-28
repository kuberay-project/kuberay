package schedulerplugins

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ktypes "k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	schedulerinterface "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/interface"
	batchschedulerutils "github.com/ray-project/kuberay/ray-operator/controllers/ray/batchscheduler/utils"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

const (
	// This is the batchScheduler name used in the Ray Operator.
	// We use this name because it is easier to understand and remember.
	// It is also consistent with the name used in the Helm chart values.yaml.
	schedulerName string = "scheduler-plugins"
	// The default scheduler plugins name is "scheduler-plugins-scheduler".
	// https://github.com/kubernetes-sigs/scheduler-plugins/blob/b3127ba4cc420430ca5322740103220043697eec/manifests/install/charts/as-a-second-scheduler/values.yaml#L6C9-L6C36
	schedulerInstanceName         string = "scheduler-plugins-scheduler"
	kubeSchedulerPodGroupLabelKey string = "scheduling.x-k8s.io/pod-group"
)

type KubeScheduler struct {
	cli client.Client
}

type KubeSchedulerFactory struct{}

func GetPluginName() string {
	return schedulerName
}

func (k *KubeScheduler) Name() string {
	return schedulerInstanceName
}

func createPodGroup(app *rayv1.RayCluster) *v1alpha1.PodGroup {
	// TODO(troychiu): Consider the case when autoscaling is enabled.

	podGroup := &v1alpha1.PodGroup{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: app.Namespace,
			Name:      app.Name,
			OwnerReferences: []metav1.OwnerReference{
				{
					Name:       app.Name,
					UID:        app.UID,
					APIVersion: app.APIVersion,
					Kind:       app.Kind,
				},
			},
		},
		Spec: v1alpha1.PodGroupSpec{
			MinMember:    utils.CalculateDesiredReplicas(app) + 1, // +1 for the head pod
			MinResources: utils.CalculateDesiredResources(app),
		},
	}
	return podGroup
}

func (k *KubeScheduler) DoBatchSchedulingOnSubmission(ctx context.Context, object metav1.Object) error {
	app, ok := object.(*rayv1.RayCluster)
	if !ok {
		return fmt.Errorf("currently only RayCluster is supported, got %T", object)
	}
	if !k.isGangSchedulingEnabled(app) {
		return nil
	}
	podGroup := &v1alpha1.PodGroup{}
	if err := k.cli.Get(ctx, ktypes.NamespacedName{Namespace: app.Namespace, Name: app.Name}, podGroup); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		podGroup = createPodGroup(app)
		if err := k.cli.Create(ctx, podGroup); err != nil {
			if errors.IsAlreadyExists(err) {
				return nil
			}
			return fmt.Errorf("failed to create PodGroup: %w", err)
		}
	}
	return nil
}

// AddMetadataToChildResource adds essential labels and annotations to the child resource.
// the scheduler needs these labels and annotations in order to do the scheduling properly
func (k *KubeScheduler) AddMetadataToChildResource(_ context.Context, parent metav1.Object, child metav1.Object, _ string) {
	// when gang scheduling is enabled, extra labels need to be added to all child resources
	if k.isGangSchedulingEnabled(parent) {
		labels := child.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[kubeSchedulerPodGroupLabelKey] = parent.GetName()
		child.SetLabels(labels)
	}
	batchschedulerutils.AddSchedulerNameToObject(child, k.Name())
}

func (k *KubeScheduler) isGangSchedulingEnabled(obj metav1.Object) bool {
	_, exist := obj.GetLabels()[utils.RayGangSchedulingEnabled]
	return exist
}

func (k *KubeScheduler) CleanupOnCompletion(_ context.Context, _ metav1.Object) (bool, error) {
	// KubeScheduler doesn't need cleanup
	return false, nil
}

func (kf *KubeSchedulerFactory) New(_ context.Context, _ *rest.Config, cli client.Client) (schedulerinterface.BatchScheduler, error) {
	if err := v1alpha1.AddToScheme(cli.Scheme()); err != nil {
		return nil, err
	}
	return &KubeScheduler{
		cli: cli,
	}, nil
}

func (kf *KubeSchedulerFactory) AddToScheme(sche *runtime.Scheme) {
	utilruntime.Must(v1alpha1.AddToScheme(sche))
}

func (kf *KubeSchedulerFactory) ConfigureReconciler(b *builder.Builder) *builder.Builder {
	return b
}
