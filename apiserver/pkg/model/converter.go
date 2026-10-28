package model

import (
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"

	"google.golang.org/protobuf/types/known/timestamppb"
	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	klog "k8s.io/klog/v2"

	"github.com/ray-project/kuberay/apiserver/pkg/util"
	api "github.com/ray-project/kuberay/proto/go_client"
	rayv1api "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	pkgutils "github.com/ray-project/kuberay/ray-operator/pkg/utils"
)

// Default annotations used by Ray nodes
func getNodeDefaultAnnotations() []string {
	return []string{
		"ray.io/compute-image",
		"openshift.io/scc",
		"cni.projectcalico.org/podIP",
		"ray.io/health-state",
		"ray.io/ft-enabled",
		"cni.projectcalico.org/podIPs",
		"cni.projectcalico.org/containerID",
		"ray.io/compute-template",
		"k8s.v1.cni.cncf.io/network-status",
		"k8s.v1.cni.cncf.io/networks-status",
	}
}

// Default labels used by Ray nodes
func getNodeDefaultLabels() []string {
	return []string{
		"app.kubernetes.io/created-by",
		"app.kubernetes.io/name",
		"ray.io/cluster",
		"ray.io/cluster-dashboard",
		"ray.io/group",
		"ray.io/identifier",
		"ray.io/is-ray-node",
		"ray.io/node-type",
	}
}

// Default env used by Ray head nodes
func getHeadNodeEnv() []string {
	return []string{
		"MY_POD_IP",
		"RAY_CLUSTER_NAME",
		"RAY_PORT",
		"RAY_ADDRESS",
		"RAY_USAGE_STATS_KUBERAY_IN_USE",
	}
}

// Default env used by Ray worker nodes
func getWorkNodeEnv() []string {
	return []string{
		"RAY_DISABLE_DOCKER_CPU_WARNING",
		"TYPE",
		"CPU_REQUEST",
		"CPU_LIMITS",
		"MEMORY_REQUESTS",
		"MEMORY_LIMITS",
		"MY_POD_NAME",
		"MY_POD_IP",
		"FQ_RAY_IP",
		"RAY_IP",
		"RAY_CLUSTER_NAME",
		"RAY_PORT",
		"RAY_ADDRESS",
		"RAY_USAGE_STATS_KUBERAY_IN_USE",
		"REDIS_PASSWORD",
	}
}

// Check if an array contains string
func contains(s []string, str string) bool {
	return slices.Contains(s, str)
}

// An events.k8s.io/v1 Event exposes the same timing/count data through two
// parallel sets of fields, and which set is populated depends on who wrote it:
//   - new events API - EventTime, Series{Count, LastObservedTime}
//   - core v1 API -    DeprecatedFirstTimestamp, DeprecatedLastTimestamp, DeprecatedCount
// The helpers below ensure that we always return the correct values regardless of which API wrote the event.

// getFirstEventTimestamp returns the event's first-observed time.
func getFirstEventTimestamp(event eventsv1.Event) *timestamppb.Timestamp {
	t := event.EventTime.Time // modern: first observed
	if t.IsZero() {
		t = event.DeprecatedFirstTimestamp.Time // legacy core v1 events
	}
	return &timestamppb.Timestamp{Seconds: t.Unix()}
}

// getLastEventTimestamp returns the event's last-observed time.
func getLastEventTimestamp(event eventsv1.Event) *timestamppb.Timestamp {
	t := event.EventTime.Time // fallback: single, non-aggregated event
	if event.Series != nil {
		t = event.Series.LastObservedTime.Time // modern: aggregated last-seen
	} else if !event.DeprecatedLastTimestamp.Time.IsZero() {
		t = event.DeprecatedLastTimestamp.Time // legacy core v1 events
	}
	return &timestamppb.Timestamp{Seconds: t.Unix()}
}

// getEventCount returns how many times the event has occurred.
func getEventCount(event eventsv1.Event) int32 {
	if event.Series != nil {
		return event.Series.Count // modern: aggregated occurrence count
	}
	return event.DeprecatedCount // legacy core v1 events
}

func FromCrdToAPIClusters(clusters []*rayv1api.RayCluster, clusterEventsMap map[string][]eventsv1.Event) []*api.Cluster {
	apiClusters := make([]*api.Cluster, 0, len(clusters))
	for _, cluster := range clusters {
		apiClusters = append(apiClusters, FromCrdToAPICluster(cluster, clusterEventsMap[cluster.Name]))
	}
	return apiClusters
}

func FromCrdToAPICluster(cluster *rayv1api.RayCluster, events []eventsv1.Event) *api.Cluster {
	pbCluster := &api.Cluster{
		Name:      cluster.Name,
		Namespace: cluster.Namespace,
		Version:   cluster.Labels[util.RayClusterVersionLabelKey],
		User:      cluster.Labels[util.RayClusterUserLabelKey],
		Environment: api.Cluster_Environment(
			api.Cluster_Environment_value[cluster.Labels[util.RayClusterEnvironmentLabelKey]],
		),
		CreatedAt:    &timestamppb.Timestamp{Seconds: cluster.CreationTimestamp.Unix()},
		ClusterState: string(cluster.Status.State),
	}

	if len(cluster.ObjectMeta.Annotations) > 0 {
		pbCluster.Annotations = cluster.ObjectMeta.Annotations
	}

	// loop container and find the resource
	pbCluster.ClusterSpec = PopulateRayClusterSpec(cluster.Spec)

	// parse events
	for _, event := range events {
		clusterEvent := &api.ClusterEvent{
			Id:             event.Name,
			Name:           fmt.Sprintf("%s-%s", cluster.Labels[util.RayClusterNameLabelKey], event.Name),
			CreatedAt:      &timestamppb.Timestamp{Seconds: event.ObjectMeta.CreationTimestamp.Unix()},
			FirstTimestamp: getFirstEventTimestamp(event),
			LastTimestamp:  getLastEventTimestamp(event),
			Reason:         event.Reason,
			Message:        event.Note,
			Type:           event.Type,
			Count:          getEventCount(event),
		}
		pbCluster.Events = append(pbCluster.Events, clusterEvent)
	}

	pbCluster.ServiceEndpoint = map[string]string{}
	maps.Copy(pbCluster.ServiceEndpoint, cluster.Status.Endpoints)
	return pbCluster
}

func PopulateRayClusterSpec(spec rayv1api.RayClusterSpec) *api.ClusterSpec {
	clusterSpec := &api.ClusterSpec{}
	clusterSpec.HeadGroupSpec = PopulateHeadNodeSpec(spec.HeadGroupSpec)
	clusterSpec.WorkerGroupSpec = PopulateWorkerNodeSpec(spec.WorkerGroupSpecs)
	if spec.EnableInTreeAutoscaling != nil && *spec.EnableInTreeAutoscaling {
		clusterSpec.EnableInTreeAutoscaling = true
		clusterSpec.AutoscalerOptions = convertAutoscalingOptions(spec.AutoscalerOptions)
	}
	clusterSpec.HeadServiceAnnotations = spec.HeadServiceAnnotations
	return clusterSpec
}

func convertAutoscalingOptions(opts *rayv1api.AutoscalerOptions) *api.AutoscalerOptions {
	if opts == nil {
		return nil
	}
	options := api.AutoscalerOptions{}
	if opts.IdleTimeoutSeconds != nil {
		options.IdleTimeoutSeconds = *opts.IdleTimeoutSeconds
	}
	if opts.UpscalingMode != nil {
		options.UpscalingMode = string(*opts.UpscalingMode)
	}
	if opts.Image != nil {
		options.Image = *opts.Image
	}
	if opts.ImagePullPolicy != nil {
		options.ImagePullPolicy = string(*opts.ImagePullPolicy)
	}
	if len(opts.Env) > 0 || len(opts.EnvFrom) > 0 {
		options.Envs = &api.EnvironmentVariables{}
		if len(opts.Env) > 0 {
			options.Envs.Values = map[string]string{}
			for _, elem := range opts.Env {
				options.Envs.Values[elem.Name] = elem.Value
			}
		}
		if len(opts.EnvFrom) > 0 {
			options.Envs.ValuesFrom = map[string]*api.EnvValueFrom{}
			for i, elem := range opts.EnvFrom {
				if elem.ConfigMapRef != nil {
					options.Envs.ValuesFrom[strconv.Itoa(i)] = &api.EnvValueFrom{
						Source: api.EnvValueFrom_CONFIGMAP,
						Name:   elem.ConfigMapRef.Name,
					}
				} else {
					options.Envs.ValuesFrom[strconv.Itoa(i)] = &api.EnvValueFrom{
						Source: api.EnvValueFrom_SECRET,
						Name:   elem.SecretRef.Name,
					}
				}
			}
		}
	}
	if len(opts.VolumeMounts) > 0 {
		options.Volumes = make([]*api.Volume, len(opts.VolumeMounts))
		for i, v := range opts.VolumeMounts {
			options.Volumes[i] = &api.Volume{
				Name:                 v.Name,
				MountPath:            v.MountPath,
				ReadOnly:             v.ReadOnly,
				MountPropagationMode: GetVolumeMountPropagation(&v),
			}
		}
	}
	if opts.Resources != nil {
		rlist := opts.Resources.Limits
		if len(rlist) == 0 {
			rlist = opts.Resources.Requests
		}
		if cc := rlist.Cpu().String(); cc != "" {
			options.Cpu = cc
		}
		if cm := rlist.Memory().String(); cm != "" {
			options.Memory = cm
		}
	}

	return &options
}

func PopulateHeadNodeSpec(spec rayv1api.HeadGroupSpec) *api.HeadGroupSpec {
	headNodeSpec := &api.HeadGroupSpec{
		RayStartParams:  spec.RayStartParams,
		ServiceType:     string(spec.ServiceType),
		Image:           spec.Template.Annotations[util.RayClusterImageAnnotationKey],
		ComputeTemplate: spec.Template.Annotations[util.RayClusterComputeTemplateAnnotationKey],
		Volumes:         PopulateVolumes(&spec.Template),
	}

	for _, annotation := range getNodeDefaultAnnotations() {
		delete(spec.Template.Annotations, annotation)
	}
	if len(spec.Template.Annotations) > 0 {
		headNodeSpec.Annotations = spec.Template.Annotations
	}

	for _, label := range getNodeDefaultLabels() {
		delete(spec.Template.Labels, label)
	}
	if len(spec.Template.Labels) > 0 {
		headNodeSpec.Labels = spec.Template.Labels
	}

	if spec.EnableIngress != nil && *spec.EnableIngress {
		headNodeSpec.EnableIngress = true
	}

	// Here we update environment and security context only for a container named 'ray-head'
	if container, _, ok := util.GetContainerByName(spec.Template.Spec.Containers, "ray-head"); ok {
		if len(container.Env) > 0 {
			headNodeSpec.Environment = convertEnvVariables(container.Env, true)
		}
		headNodeSpec.SecurityContext = convertSecurityContext(container.SecurityContext)
	}

	if len(spec.Template.Spec.ServiceAccountName) > 1 {
		headNodeSpec.ServiceAccount = spec.Template.Spec.ServiceAccountName
	}

	if len(spec.Template.Spec.ImagePullSecrets) > 0 {
		headNodeSpec.ImagePullSecret = spec.Template.Spec.ImagePullSecrets[0].Name
	}
	if spec.Template.Spec.Containers[0].ImagePullPolicy == corev1.PullAlways {
		headNodeSpec.ImagePullPolicy = "Always"
	}

	return headNodeSpec
}

func PopulateWorkerNodeSpec(specs []rayv1api.WorkerGroupSpec) []*api.WorkerGroupSpec {
	var workerNodeSpecs []*api.WorkerGroupSpec

	for _, spec := range specs {
		workerNodeSpec := &api.WorkerGroupSpec{
			RayStartParams:  spec.RayStartParams,
			MaxReplicas:     *spec.MaxReplicas,
			MinReplicas:     *spec.MinReplicas,
			Replicas:        *spec.Replicas,
			GroupName:       spec.GroupName,
			Image:           spec.Template.Annotations[util.RayClusterImageAnnotationKey],
			ComputeTemplate: spec.Template.Annotations[util.RayClusterComputeTemplateAnnotationKey],
			Volumes:         PopulateVolumes(&spec.Template),
		}

		for _, annotation := range getNodeDefaultAnnotations() {
			delete(spec.Template.Annotations, annotation)
		}
		if len(spec.Template.Annotations) > 0 {
			workerNodeSpec.Annotations = spec.Template.Annotations
		}

		for _, label := range getNodeDefaultLabels() {
			delete(spec.Template.Labels, label)
		}
		if len(spec.Template.Labels) > 0 {
			workerNodeSpec.Labels = spec.Template.Labels
		}

		// Here we update environment and security context only for a container named 'ray-worker'
		if container, _, ok := util.GetContainerByName(spec.Template.Spec.Containers, "ray-worker"); ok {
			if len(container.Env) > 0 {
				workerNodeSpec.Environment = convertEnvVariables(container.Env, false)
			}
			workerNodeSpec.SecurityContext = convertSecurityContext(container.SecurityContext)
		}

		if len(spec.Template.Spec.ServiceAccountName) > 1 {
			workerNodeSpec.ServiceAccount = spec.Template.Spec.ServiceAccountName
		}

		if len(spec.Template.Spec.ImagePullSecrets) > 0 {
			workerNodeSpec.ImagePullSecret = spec.Template.Spec.ImagePullSecrets[0].Name
		}
		if spec.Template.Spec.Containers[0].ImagePullPolicy == corev1.PullAlways {
			workerNodeSpec.ImagePullPolicy = "Always"
		}

		workerNodeSpecs = append(workerNodeSpecs, workerNodeSpec)
	}

	return workerNodeSpecs
}

func convertSecurityContext(securityCtx *corev1.SecurityContext) *api.SecurityContext {
	if securityCtx == nil {
		return nil
	}
	result := &api.SecurityContext{
		Privileged:   securityCtx.Privileged,
		Capabilities: &api.Capabilities{},
	}
	if securityCtx.Capabilities != nil {
		for _, cap := range securityCtx.Capabilities.Add {
			result.Capabilities.Add = append(result.Capabilities.Add, string(cap))
		}
		for _, cap := range securityCtx.Capabilities.Drop {
			result.Capabilities.Drop = append(result.Capabilities.Drop, string(cap))
		}
	}
	return result
}

func convertEnvVariables(cenv []corev1.EnvVar, header bool) *api.EnvironmentVariables {
	env := api.EnvironmentVariables{
		Values:     make(map[string]string),
		ValuesFrom: make(map[string]*api.EnvValueFrom),
	}
	for _, kv := range cenv {
		if header {
			if contains(getHeadNodeEnv(), kv.Name) {
				continue
			}
		} else {
			if contains(getWorkNodeEnv(), kv.Name) {
				// Skip reserved names
				continue
			}
		}
		if kv.ValueFrom != nil {
			// this is value from
			if kv.ValueFrom.ConfigMapKeyRef != nil {
				// This is config map
				env.ValuesFrom[kv.Name] = &api.EnvValueFrom{
					Source: api.EnvValueFrom_CONFIGMAP,
					Name:   kv.ValueFrom.ConfigMapKeyRef.Name,
					Key:    kv.ValueFrom.ConfigMapKeyRef.Key,
				}
				continue
			}
			if kv.ValueFrom.SecretKeyRef != nil {
				// This is Secret
				env.ValuesFrom[kv.Name] = &api.EnvValueFrom{
					Source: api.EnvValueFrom_SECRET,
					Name:   kv.ValueFrom.SecretKeyRef.Name,
					Key:    kv.ValueFrom.SecretKeyRef.Key,
				}
				continue
			}
			if kv.ValueFrom.ResourceFieldRef != nil {
				// This resource ref
				env.ValuesFrom[kv.Name] = &api.EnvValueFrom{
					Source: api.EnvValueFrom_RESOURCEFIELD,
					Name:   kv.ValueFrom.ResourceFieldRef.ContainerName,
					Key:    kv.ValueFrom.ResourceFieldRef.Resource,
				}
				continue
			}
			if kv.ValueFrom.FieldRef != nil {
				// This resource ref
				env.ValuesFrom[kv.Name] = &api.EnvValueFrom{
					Source: api.EnvValueFrom_FIELD,
					Key:    kv.ValueFrom.FieldRef.FieldPath,
				}
				continue
			}
		} else {
			// This is value
			env.Values[kv.Name] = kv.Value
		}
	}
	return &env
}

func FromKubeToAPIComputeTemplate(configMap *corev1.ConfigMap) *api.ComputeTemplate {
	cpu, _ := strconv.ParseUint(configMap.Data["cpu"], 10, 32)
	memory, _ := strconv.ParseUint(configMap.Data["memory"], 10, 32)
	gpu, _ := strconv.ParseUint(configMap.Data["gpu"], 10, 32)

	runtime := &api.ComputeTemplate{}
	runtime.Name = configMap.Name
	runtime.Namespace = configMap.Namespace
	runtime.Cpu = uint32(cpu)
	runtime.Memory = uint32(memory)
	runtime.MemoryUnit = configMap.Data["memory_unit"]
	runtime.Gpu = uint32(gpu)
	runtime.GpuAccelerator = configMap.Data["gpu_accelerator"]

	val, ok := configMap.Data["extended_resources"]
	if ok {
		err := json.Unmarshal(pkgutils.ConvertStringToByteSlice(val), &runtime.ExtendedResources)
		if err != nil {
			klog.Error("failed to unmarshall extended resources for compute template ", runtime.Name, " value ",
				runtime.ExtendedResources, " error ", err)
		}
	}

	val, ok = configMap.Data["tolerations"]
	if ok {
		err := json.Unmarshal(pkgutils.ConvertStringToByteSlice(val), &runtime.Tolerations)
		if err != nil {
			klog.Error("failed to unmarshall tolerations for compute template ", runtime.Name, " value ",
				runtime.Tolerations, " error ", err)
		}
	}
	return runtime
}

func FromKubeToAPIComputeTemplates(configMaps []*corev1.ConfigMap) []*api.ComputeTemplate {
	apiComputeTemplates := make([]*api.ComputeTemplate, 0, len(configMaps))
	for _, configMap := range configMaps {
		apiComputeTemplates = append(apiComputeTemplates, FromKubeToAPIComputeTemplate(configMap))
	}
	return apiComputeTemplates
}

func FromCrdToAPIJobs(jobs []*rayv1api.RayJob) []*api.RayJob {
	apiJobs := make([]*api.RayJob, 0, len(jobs))
	for _, job := range jobs {
		apiJobs = append(apiJobs, FromCrdToAPIJob(job))
	}
	return apiJobs
}

func FromCrdToAPIJob(job *rayv1api.RayJob) (pbJob *api.RayJob) {
	defer func() {
		err := recover()
		if err != nil {
			klog.Errorf("failed to transfer job crd to job protobuf, err: %v, crd: %+v", err, job)
		}
	}()

	var version string
	if job.Spec.RayClusterSpec != nil {
		version = job.Spec.RayClusterSpec.RayVersion
	}
	pbJob = &api.RayJob{
		Name:                     job.Name,
		Namespace:                job.Namespace,
		User:                     job.Labels[util.RayClusterUserLabelKey],
		Version:                  version,
		Entrypoint:               job.Spec.Entrypoint,
		Metadata:                 job.Spec.Metadata,
		RuntimeEnv:               job.Spec.RuntimeEnvYAML,
		JobId:                    job.Status.JobId,
		ShutdownAfterJobFinishes: job.Spec.ShutdownAfterJobFinishes,
		CreatedAt:                &timestamppb.Timestamp{Seconds: job.CreationTimestamp.Unix()},
		JobStatus:                string(job.Status.JobStatus),
		JobDeploymentStatus:      string(job.Status.JobDeploymentStatus),
		Message:                  job.Status.Message,
	}

	// Add optional params
	if job.Spec.ClusterSelector != nil {
		pbJob.ClusterSelector = job.Spec.ClusterSelector
	}

	if job.Spec.RayClusterSpec != nil {
		pbJob.ClusterSpec = PopulateRayClusterSpec(*job.Spec.RayClusterSpec)
	}

	pbJob.TtlSecondsAfterFinished = job.Spec.TTLSecondsAfterFinished

	if job.DeletionTimestamp != nil {
		pbJob.DeleteAt = &timestamppb.Timestamp{Seconds: job.DeletionTimestamp.Unix()}
	}

	if job.Spec.SubmitterPodTemplate != nil {
		pbJob.JobSubmitter = &api.RayJobSubmitter{
			Image: job.Spec.SubmitterPodTemplate.Spec.Containers[0].Image,
		}
		if cpu := job.Spec.SubmitterPodTemplate.Spec.Containers[0].Resources.Limits.Cpu().String(); cpu != "1" {
			pbJob.JobSubmitter.Cpu = cpu
		}
		if mem := job.Spec.SubmitterPodTemplate.Spec.Containers[0].Resources.Limits.Memory().String(); mem != "1Gi" {
			pbJob.JobSubmitter.Memory = mem
		}
	}
	if jcpus := job.Spec.EntrypointNumCpus; jcpus > 0 {
		pbJob.EntrypointNumCpus = jcpus
	}
	if jgpus := job.Spec.EntrypointNumGpus; jgpus > 0 {
		pbJob.EntrypointNumGpus = jgpus
	}
	if jres := job.Spec.EntrypointResources; jres != "" {
		pbJob.EntrypointResources = jres
	}

	if jstarttime := job.Status.StartTime; jstarttime != nil {
		pbJob.StartTime = timestamppb.New(job.Status.StartTime.Time)
	}
	if jendtime := job.Status.EndTime; jendtime != nil {
		pbJob.EndTime = timestamppb.New(job.Status.EndTime.Time)
	}
	if jclustername := job.Status.RayClusterName; jclustername != "" {
		pbJob.RayClusterName = jclustername
	}

	return pbJob
}

func FromCrdToAPIServices(
	services []*rayv1api.RayService,
	serviceEventsMap map[string][]eventsv1.Event,
) []*api.RayService {
	apiServices := make([]*api.RayService, 0, len(services))
	for _, service := range services {
		apiServices = append(apiServices, FromCrdToAPIService(service, serviceEventsMap[service.Name]))
	}
	return apiServices
}

func FromCrdToAPIService(service *rayv1api.RayService, events []eventsv1.Event) *api.RayService {
	defer func() {
		err := recover()
		if err != nil {
			klog.Errorf("failed to transfer ray service, err: %v, item: %v", err, service)
		}
	}()

	var deleteTime int64 = -1
	if service.DeletionTimestamp != nil {
		deleteTime = service.DeletionTimestamp.Unix()
	}
	pbService := &api.RayService{
		Name:           service.Name,
		Namespace:      service.Namespace,
		User:           service.Labels[util.RayClusterUserLabelKey],
		Version:        service.Spec.RayClusterSpec.RayVersion,
		ServeConfig_V2: service.Spec.ServeConfigV2,
		ClusterSpec:    PopulateRayClusterSpec(service.Spec.RayClusterSpec),
		ServiceUnhealthySecondThreshold: PoplulateUnhealthySecondThreshold(
			service.Spec.ServiceUnhealthySecondThreshold,
		),
		DeploymentUnhealthySecondThreshold: PoplulateUnhealthySecondThreshold(
			service.Spec.DeploymentUnhealthySecondThreshold,
		),
		RayServiceStatus: PoplulateRayServiceStatus(service.Name, service.Status, events),
		CreatedAt:        &timestamppb.Timestamp{Seconds: service.CreationTimestamp.Unix()},
		DeleteAt:         &timestamppb.Timestamp{Seconds: deleteTime},
	}
	return pbService
}

func PoplulateUnhealthySecondThreshold(value *int32) int32 {
	if value == nil {
		return 0
	}
	return *value
}

func PoplulateRayServiceStatus(
	serviceName string,
	serviceStatus rayv1api.RayServiceStatuses,
	events []eventsv1.Event,
) *api.RayServiceStatus {
	status := &api.RayServiceStatus{
		RayServiceEvents:       PopulateRayServiceEvent(serviceName, events),
		RayClusterName:         serviceStatus.ActiveServiceStatus.RayClusterName,
		RayClusterState:        string(serviceStatus.ActiveServiceStatus.RayClusterStatus.State),
		ServeApplicationStatus: PopulateServeApplicationStatus(serviceStatus.ActiveServiceStatus.Applications),
	}
	status.ServiceEndpoint = map[string]string{}
	maps.Copy(status.ServiceEndpoint, serviceStatus.ActiveServiceStatus.RayClusterStatus.Endpoints)
	return status
}

func PopulateServeApplicationStatus(
	serveApplicationStatuses map[string]rayv1api.AppStatus,
) []*api.ServeApplicationStatus {
	appStatuses := make([]*api.ServeApplicationStatus, 0, len(serveApplicationStatuses))
	for appName, appStatus := range serveApplicationStatuses {
		ds := &api.ServeApplicationStatus{
			Name:                  appName,
			Status:                appStatus.Status,
			Message:               appStatus.Message,
			ServeDeploymentStatus: PopulateServeDeploymentStatus(appStatus.Deployments),
		}
		appStatuses = append(appStatuses, ds)
	}
	return appStatuses
}

func PopulateServeDeploymentStatus(
	serveDeploymentStatuses map[string]rayv1api.ServeDeploymentStatus,
) []*api.ServeDeploymentStatus {
	deploymentStatuses := make([]*api.ServeDeploymentStatus, 0, len(serveDeploymentStatuses))
	for deploymentName, deploymentStatus := range serveDeploymentStatuses {
		ds := &api.ServeDeploymentStatus{
			DeploymentName: deploymentName,
			Status:         deploymentStatus.Status,
			Message:        deploymentStatus.Message,
		}
		deploymentStatuses = append(deploymentStatuses, ds)
	}
	return deploymentStatuses
}

func PopulateRayServiceEvent(serviceName string, events []eventsv1.Event) []*api.RayServiceEvent {
	serviceEvents := make([]*api.RayServiceEvent, 0, len(events))
	for _, event := range events {
		serviceEvent := &api.RayServiceEvent{
			Id:             event.Name,
			Name:           fmt.Sprintf("%s-%s", serviceName, event.Name),
			CreatedAt:      &timestamppb.Timestamp{Seconds: event.ObjectMeta.CreationTimestamp.Unix()},
			FirstTimestamp: getFirstEventTimestamp(event),
			LastTimestamp:  getLastEventTimestamp(event),
			Reason:         event.Reason,
			Message:        event.Note,
			Type:           event.Type,
			Count:          getEventCount(event),
		}
		serviceEvents = append(serviceEvents, serviceEvent)
	}
	return serviceEvents
}
