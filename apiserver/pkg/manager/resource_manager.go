package manager

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	eventsv1client "k8s.io/client-go/kubernetes/typed/events/v1"

	"github.com/ray-project/kuberay/apiserver/pkg/model"
	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	rayv1 "github.com/ray-project/kuberay/ray-operator/pkg/client/clientset/versioned/typed/ray/v1"
)

const DefaultNamespace = "ray-system"

type ResourceManager struct {
	clientManager ClientManagerInterface
}

// It would be easier to discover methods.
func NewResourceManager(clientManager ClientManagerInterface) *ResourceManager {
	return &ResourceManager{
		clientManager: clientManager,
	}
}

// Clients
func (r *ResourceManager) getRayClusterClient(namespace string) rayv1.RayClusterInterface {
	return r.clientManager.ClusterClient().RayClusterClient(namespace)
}

func (r *ResourceManager) getRayJobClient(namespace string) rayv1.RayJobInterface {
	return r.clientManager.JobClient().RayJobClient(namespace)
}

func (r *ResourceManager) getRayServiceClient(namespace string) rayv1.RayServiceInterface {
	return r.clientManager.ServiceClient().RayServiceClient(namespace)
}

func (r *ResourceManager) getKubernetesConfigMapClient(namespace string) clientv1.ConfigMapInterface {
	return r.clientManager.KubernetesClient().ConfigMapClient(namespace)
}

func (r *ResourceManager) getEventsClient(namespace string) eventsv1client.EventInterface {
	return r.clientManager.KubernetesClient().EventsClient(namespace)
}

func (r *ResourceManager) getKubernetesNamespaceClient() clientv1.NamespaceInterface {
	return r.clientManager.KubernetesClient().NamespaceClient()
}

// clusters
func (r *ResourceManager) CreateCluster(ctx context.Context, apiCluster *api.Cluster) (*rayv1api.RayCluster, error) {
	// populate cluster map
	computeTemplateDict, err := r.PopulateComputeTemplate(ctx, apiCluster.ClusterSpec, apiCluster.Namespace)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to populate compute template for (%s/%s)", apiCluster.Namespace, apiCluster.Name)
	}

	// convert *api.Cluster to rayv1api.RayCluster
	rayCluster, err := util.NewRayCluster(apiCluster, computeTemplateDict)
	if err != nil {
		return nil, util.NewInvalidInputErrorWithDetails(err, "Failed to create a Ray cluster")
	}

	// set our own fields.
	clusterAt := r.clientManager.Time().Now().String()
	rayCluster.Annotations["ray.io/creation-timestamp"] = clusterAt

	newRayCluster, err := r.getRayClusterClient(apiCluster.Namespace).Create(ctx, rayCluster.Get(), metav1.CreateOptions{})
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create a cluster for (%s/%s)", rayCluster.Namespace, rayCluster.Name)
	}

	return newRayCluster, nil
}

// Compute template
func (r *ResourceManager) PopulateComputeTemplate(ctx context.Context, clusterSpec *api.ClusterSpec, nameSpace string) (map[string]*api.ComputeTemplate, error) {
	dict := map[string]*api.ComputeTemplate{}
	// populate head compute template
	name := clusterSpec.HeadGroupSpec.ComputeTemplate
	configMap, err := r.GetComputeTemplate(ctx, name, nameSpace)
	if err != nil {
		return nil, fmt.Errorf("Cannot get compute template for name '%s' in namespace '%s', error: %w", name, nameSpace, err)
	}
	computeTemplate := model.FromKubeToAPIComputeTemplate(configMap)
	dict[name] = computeTemplate

	// populate worker compute template
	for _, spec := range clusterSpec.WorkerGroupSpec {
		name := spec.ComputeTemplate
		if _, exist := dict[name]; !exist {
			configMap, err := r.GetComputeTemplate(ctx, name, nameSpace)
			if err != nil {
				return nil, fmt.Errorf("Cannot get compute template for name '%s' in namespace '%s', error: %w", name, nameSpace, err)
			}
			computeTemplate := model.FromKubeToAPIComputeTemplate(configMap)
			dict[name] = computeTemplate
		}
	}

	return dict, nil
}

func (r *ResourceManager) GetCluster(ctx context.Context, clusterName string, namespace string) (*rayv1api.RayCluster, error) {
	client := r.getRayClusterClient(namespace)
	return getClusterByName(ctx, client, clusterName)
}

func (r *ResourceManager) ListClusters(ctx context.Context, namespace string, continueToken string, limit int64) ([]*rayv1api.RayCluster, string, error) {
	labelSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{
			util.KubernetesManagedByLabelKey: util.ComponentName,
		},
	}
	rayClusterList, err := r.getRayClusterClient(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set(labelSelector.MatchLabels).String(),
		Limit:         limit,
		Continue:      continueToken,
	})
	if err != nil {
		return nil, "", util.Wrap(err, fmt.Sprintf("List RayCluster failed in %s", namespace))
	}

	var result []*rayv1api.RayCluster
	length := len(rayClusterList.Items)
	for i := range length {
		result = append(result, &rayClusterList.Items[i])
	}

	return result, rayClusterList.Continue, nil
}

func (r *ResourceManager) DeleteCluster(ctx context.Context, clusterName string, namespace string) error {
	client := r.getRayClusterClient(namespace)
	cluster, err := getClusterByName(ctx, client, clusterName)
	if err != nil {
		return util.Wrap(err, "Get cluster failure")
	}

	// Delete Kubernetes resources
	if err := client.Delete(ctx, cluster.Name, metav1.DeleteOptions{}); err != nil {
		// API won't need to delete the ray cluster CR
		return util.NewInternalServerError(err, "Failed to delete cluster %v.", clusterName)
	}

	return nil
}

func (r *ResourceManager) CreateJob(ctx context.Context, apiJob *api.RayJob) (*rayv1api.RayJob, error) {
	computeTemplateMap := make(map[string]*api.ComputeTemplate)
	var err error

	// populate cluster map
	if apiJob.ClusterSpec != nil {
		computeTemplateMap, err = r.PopulateComputeTemplate(ctx, apiJob.ClusterSpec, apiJob.Namespace)
		if err != nil {
			return nil, util.NewInternalServerError(err, "Failed to populate compute template for (%s/%s)", apiJob.Namespace, apiJob.JobId)
		}
	}

	// convert *api.Cluster to rayv1api.RayCluster
	rayJob, err := util.NewRayJob(apiJob, computeTemplateMap)
	if err != nil {
		return nil, util.NewInvalidInputErrorWithDetails(err, "Failed to create a Ray Job")
	}

	newRayJob, err := r.getRayJobClient(apiJob.Namespace).Create(ctx, rayJob.Get(), metav1.CreateOptions{})
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create a job for (%s/%s)", apiJob.Namespace, apiJob.JobId)
	}

	return newRayJob, nil
}

func (r *ResourceManager) GetJob(ctx context.Context, jobName string, namespace string) (*rayv1api.RayJob, error) {
	client := r.getRayJobClient(namespace)
	return getJobByName(ctx, client, jobName)
}

func (r *ResourceManager) ListJobs(ctx context.Context, namespace string, continueToken string, limit int64) ([]*rayv1api.RayJob, string /* continue token */, error) {
	labelSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{
			util.KubernetesManagedByLabelKey: util.ComponentName,
		},
	}
	rayJobList, err := r.getRayJobClient(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set(labelSelector.MatchLabels).String(),
		Limit:         limit,
		Continue:      continueToken,
	})
	if err != nil {
		return nil, "", util.Wrap(err, fmt.Sprintf("List RayCluster failed in %s with Limit %d and ContinueToken %s", namespace, limit, continueToken))
	}

	var result []*rayv1api.RayJob
	length := len(rayJobList.Items)
	for i := range length {
		result = append(result, &rayJobList.Items[i])
	}

	return result, rayJobList.Continue, nil
}

func (r *ResourceManager) DeleteJob(ctx context.Context, jobName string, namespace string) error {
	client := r.getRayJobClient(namespace)
	job, err := getJobByName(ctx, client, jobName)
	if err != nil {
		return util.Wrap(err, "Get job failure")
	}

	// Delete Kubernetes resources
	if err := client.Delete(ctx, job.Name, metav1.DeleteOptions{}); err != nil {
		// API won't need to delete the ray cluster CR
		return util.NewInternalServerError(err, "Failed to delete cluster %v.", jobName)
	}

	return nil
}

func (r *ResourceManager) CreateService(ctx context.Context, apiService *api.RayService) (*rayv1api.RayService, error) {
	// populate cluster map
	computeTemplateDict, err := r.PopulateComputeTemplate(ctx, apiService.ClusterSpec, apiService.Namespace)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to populate compute template for (%s/%s)", apiService.Namespace, apiService.Name)
	}
	rayService, err := util.NewRayService(apiService, computeTemplateDict)
	if err != nil {
		return nil, util.NewInvalidInputErrorWithDetails(err, "Failed to create a Ray Service")
	}
	createdAt := r.clientManager.Time().Now().String()
	rayService.Annotations["ray.io/creation-timestamp"] = createdAt
	newRayService, err := r.getRayServiceClient(apiService.Namespace).Create(ctx, rayService.Get(), metav1.CreateOptions{})
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create service for (%s/%s)", rayService.Namespace, rayService.Name)
	}

	return newRayService, nil
}

func (r *ResourceManager) UpdateRayService(ctx context.Context, apiService *api.RayService) (*rayv1api.RayService, error) {
	name := apiService.Name
	namespace := apiService.Namespace
	client := r.getRayServiceClient(namespace)
	oldService, err := getServiceByName(ctx, client, name)
	if err != nil {
		return nil, util.Wrap(err, fmt.Sprintf("Update service fail, no service named: %s ", name))
	}
	// populate cluster map
	computeTemplateDict, err := r.PopulateComputeTemplate(ctx, apiService.ClusterSpec, apiService.Namespace)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to populate compute template for (%s/%s)", apiService.Namespace, apiService.Name)
	}
	rayService, err := util.NewRayService(apiService, computeTemplateDict)
	if err != nil {
		return nil, err
	}
	rayService.Annotations["ray.io/update-timestamp"] = r.clientManager.Time().Now().String()
	rayService.ResourceVersion = oldService.DeepCopy().ResourceVersion
	newRayService, err := client.Update(ctx, rayService.Get(), metav1.UpdateOptions{})
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to update service for (%s/%s)", rayService.Namespace, rayService.Name)
	}
	return newRayService, nil
}

func (r *ResourceManager) GetService(ctx context.Context, serviceName, namespace string) (*rayv1api.RayService, error) {
	client := r.getRayServiceClient(namespace)
	return getServiceByName(ctx, client, serviceName)
}

func (r *ResourceManager) ListServices(ctx context.Context, namespace string, pageToken string, pageSize int32) ([]*rayv1api.RayService, string /*nextPageToken*/, error) {
	labelSelector := metav1.LabelSelector{
		MatchLabels: map[string]string{
			util.KubernetesManagedByLabelKey: util.ComponentName,
		},
	}
	rayServiceList, err := r.getRayServiceClient(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: labels.Set(labelSelector.MatchLabels).String(),
		Limit:         int64(pageSize),
		Continue:      pageToken,
	})
	if err != nil {
		return nil, "" /*nextPageToken*/, util.Wrap(err, fmt.Sprintf("List RayService failed in %s with next page token %s and page limit %d", namespace, pageToken, pageSize))
	}
	rayServices := make([]*rayv1api.RayService, 0)
	for _, service := range rayServiceList.Items {
		rayServices = append(rayServices, &service)
	}

	return rayServices, rayServiceList.Continue, nil
}

func (r *ResourceManager) DeleteService(ctx context.Context, serviceName, namespace string) error {
	client := r.getRayServiceClient(namespace)
	service, err := getServiceByName(ctx, client, serviceName)
	if err != nil {
		return util.Wrap(err, "delete ray service failure")
	}
	if err := client.Delete(ctx, service.Name, metav1.DeleteOptions{}); err != nil {
		return util.NewInternalServerError(err, "failed to delete ray service %s.", service.Name)
	}

	return nil
}

// Compute Runtimes
func (r *ResourceManager) CreateComputeTemplate(ctx context.Context, runtime *api.ComputeTemplate) (*corev1.ConfigMap, error) {
	_, err := r.GetComputeTemplate(ctx, runtime.Name, runtime.Namespace)
	if err == nil {
		return nil, util.NewAlreadyExistError("Compute template with name %s already exists in namespace %s", runtime.Name, runtime.Namespace)
	}

	computeTemplate, err := util.NewComputeTemplate(runtime)
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to convert compute runtime (%s/%s)", runtime.Namespace, runtime.Name)
	}

	client := r.getKubernetesConfigMapClient(runtime.Namespace)
	newRuntime, err := client.Create(ctx, computeTemplate, metav1.CreateOptions{})
	if err != nil {
		return nil, util.NewInternalServerError(err, "Failed to create a compute runtime for (%s/%s)", runtime.Namespace, runtime.Name)
	}

	return newRuntime, nil
}

func (r *ResourceManager) GetComputeTemplate(ctx context.Context, name string, namespace string) (*corev1.ConfigMap, error) {
	client := r.getKubernetesConfigMapClient(namespace)
	return getComputeTemplateByName(ctx, client, name)
}

func (r *ResourceManager) ListComputeTemplates(ctx context.Context, namespace string) ([]*corev1.ConfigMap, error) {
	client := r.getKubernetesConfigMapClient(namespace)
	configMapList, err := client.List(ctx, metav1.ListOptions{LabelSelector: "ray.io/config-type=compute-template"})
	if err != nil {
		return nil, util.Wrap(err, "List compute templates failed")
	}

	var result []*corev1.ConfigMap
	length := len(configMapList.Items)
	for i := range length {
		result = append(result, &configMapList.Items[i])
	}

	return result, nil
}

func (r *ResourceManager) ListAllComputeTemplates(ctx context.Context) ([]*corev1.ConfigMap, error) {
	namespaces, err := r.getKubernetesNamespaceClient().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, util.Wrap(err, "Failed to fetch all Kubernetes namespaces")
	}

	var result []*corev1.ConfigMap
	for _, namespace := range namespaces.Items {
		client := r.getKubernetesConfigMapClient(namespace.Name)
		configMapList, err := client.List(ctx, metav1.ListOptions{LabelSelector: "ray.io/config-type=compute-template"})
		if err != nil {
			return nil, util.Wrap(err, fmt.Sprintf("List compute templates failed in %s", namespace.Name))
		}

		length := len(configMapList.Items)
		for i := range length {
			result = append(result, &configMapList.Items[i])
		}
	}
	return result, nil
}

func (r *ResourceManager) DeleteComputeTemplate(ctx context.Context, name string, namespace string) error {
	client := r.getKubernetesConfigMapClient(namespace)

	configMap, err := getComputeTemplateByName(ctx, client, name)
	if err != nil {
		return util.Wrap(err, "Get compute template failure")
	}

	if err := client.Delete(ctx, configMap.Name, metav1.DeleteOptions{}); err != nil {
		return util.NewInternalServerError(err, "failed to delete compute template %v.", name)
	}

	return nil
}

// getClusterByName returns the Kubernetes RayCluster object by given name and client
func getClusterByName(ctx context.Context, client rayv1.RayClusterInterface, name string) (*rayv1api.RayCluster, error) {
	cluster, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, util.NewNotFoundError(err, "Cluster %s not found", name)
		}

		return nil, util.Wrap(err, "Get Cluster failed")
	}
	if managedBy, ok := cluster.Labels[util.KubernetesManagedByLabelKey]; !ok || managedBy != util.ComponentName {
		return nil, fmt.Errorf("RayCluster with name %s not managed by %s", name, util.ComponentName)
	}

	return cluster, nil
}

// getJobByName returns the Kubernetes RayJob object by given name and client
func getJobByName(ctx context.Context, client rayv1.RayJobInterface, name string) (*rayv1api.RayJob, error) {
	job, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, util.NewNotFoundError(err, "Job %s not found", name)
		}

		return nil, util.Wrap(err, "Get Job failed")
	}
	if managedBy, ok := job.Labels[util.KubernetesManagedByLabelKey]; !ok || managedBy != util.ComponentName {
		return nil, fmt.Errorf("RayCluster with name %s not managed by %s", name, util.ComponentName)
	}

	return job, nil
}

func getServiceByName(ctx context.Context, client rayv1.RayServiceInterface, name string) (*rayv1api.RayService, error) {
	service, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, util.NewNotFoundError(err, "Service %s not found", name)
		}
		return nil, util.Wrap(err, "get service failed")
	}
	if managedBy, ok := service.Labels[util.KubernetesManagedByLabelKey]; !ok || managedBy != util.ComponentName {
		return nil, fmt.Errorf("RayService with name %s not managed by %s", name, util.ComponentName)
	}
	return service, nil
}

// getComputeTemplateByName returns the Kubernetes configmap object by given name and client
func getComputeTemplateByName(ctx context.Context, client clientv1.ConfigMapInterface, name string) (*corev1.ConfigMap, error) {
	runtime, err := client.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, util.NewNotFoundError(err, "Compute template %s not found", name)
		}

		return nil, util.Wrap(err, "Get compute template failed")
	}

	return runtime, nil
}

func (r *ResourceManager) GetClusterEvents(ctx context.Context, clusterName string, namespace string) ([]eventsv1.Event, error) {
	client := r.getEventsClient(namespace)
	clusterClient := r.getRayClusterClient(namespace)
	return getRayClusterEventsByName(ctx, clusterName, client, clusterClient)
}

func getRayClusterEventsByName(ctx context.Context, name string, client eventsv1client.EventInterface, clusterClient rayv1.RayClusterInterface) ([]eventsv1.Event, error) {
	rayCluster, err := getClusterByName(ctx, clusterClient, name)
	if err != nil {
		return nil, util.Wrap(err, "get raycluster event failed")
	}
	fieldSelectorById := fmt.Sprintf("regarding.name=%s", rayCluster.Name)
	events, err := client.List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelectorById,
		TypeMeta:      metav1.TypeMeta{Kind: "RayCluster"},
	})
	if err != nil {
		return nil, util.Wrap(err, "Get Ray Cluster Events failed")
	}
	if len(events.Items) == 0 {
		return nil, fmt.Errorf("no Event with RayCluster name %s", name)
	}

	return events.Items, nil
}

func (r *ResourceManager) GetServiceEvents(ctx context.Context, service rayv1api.RayService) ([]eventsv1.Event, error) {
	eventClient := r.getEventsClient(service.Namespace)
	events, err := getRayServiceEventsByName(ctx, service.Name, eventClient)
	if err != nil {
		return nil, err
	}
	if len(service.Status.ActiveServiceStatus.RayClusterName) > 0 {
		clusterEvents, err := r.GetClusterEvents(ctx, service.Status.ActiveServiceStatus.RayClusterName, service.Namespace)
		if err != nil {
			clusterEvents = make([]eventsv1.Event, 0)
		}
		events = append(events, clusterEvents...)
	}
	return events, nil
}

func getRayServiceEventsByName(ctx context.Context, name string, client eventsv1client.EventInterface) ([]eventsv1.Event, error) {
	fieldSelectorById := fmt.Sprintf("regarding.name=%s", name)
	events, err := client.List(ctx, metav1.ListOptions{
		FieldSelector: fieldSelectorById,
		TypeMeta:      metav1.TypeMeta{Kind: "RayService"},
	})
	if err != nil {
		return nil, util.Wrap(err, "Get Ray Cluster Events failed")
	}
	if len(events.Items) == 0 {
		return make([]eventsv1.Event, 0), nil
	}

	return events.Items, nil
}
