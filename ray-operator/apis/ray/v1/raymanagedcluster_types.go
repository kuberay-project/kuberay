package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RayManagedClusterSpec is a RayCluster's spec as Ray's ray.cluster API wants it: KubeRay creates
// each group's initial replicas, and after that only the replicas Ray asks for in the status.
// +kubebuilder:validation:XValidation:rule="has(self.authOptions) == has(oldSelf.authOptions) && (!has(self.authOptions) || self.authOptions == oldSelf.authOptions)",message="authOptions is immutable: workers with other auth couldn't join the head"
type RayManagedClusterSpec struct {
	// AuthOptions: how the cluster's processes authenticate. Ray 3 requires a token by default,
	// so without it, KubeRay generates one (as with mode token and no secretName); mode disabled
	// turns authentication off. Each Ray container reads the token from the Secret, mounted as a
	// file (RAY_AUTH_TOKEN_PATH). Ray 3 has no Kubernetes token authentication.
	// +kubebuilder:validation:XValidation:rule="!has(self.enableK8sTokenAuth) || !self.enableK8sTokenAuth",message="Ray 3 has no Kubernetes token authentication"
	// +optional
	AuthOptions *AuthOptions `json:"authOptions,omitempty"`
	// HeadGroupSpec is the head pod's spec: the instance group named headgroup.
	HeadGroupSpec HeadGroupSpec `json:"headGroupSpec"`
	// RayVersion is the version of Ray the pods run.
	RayVersion string `json:"rayVersion,omitempty"`
	// WorkerGroupSpecs are the instance groups Ray adds instances to.
	// +listType=map
	// +listMapKey=groupName
	WorkerGroupSpecs []RayManagedWorkerGroupSpec `json:"workerGroupSpecs,omitempty"`
}

// RayManagedWorkerGroupSpec is a worker group: pods of one shape, made a replica at a time.
type RayManagedWorkerGroupSpec struct {
	// GroupName names the group. headgroup is reserved. It's the pods' ray.io/group label, so a label
	// value.
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?$`
	GroupName string `json:"groupName"`
	// InitialReplicas is how many replicas the group starts with when the cluster is created.
	// KubeRay doesn't keep this count afterward.
	// +kubebuilder:default:=0
	// +kubebuilder:validation:Minimum=0
	InitialReplicas *int32 `json:"initialReplicas,omitempty"`
	// MaxReplicas is the most replicas the group can have. Ray enforces it, not KubeRay.
	// +kubebuilder:default:=2147483647
	// +kubebuilder:validation:Minimum=0
	MaxReplicas *int32 `json:"maxReplicas,omitempty"`
	// NumOfHosts is the number of pods in each replica, one per host.
	// +kubebuilder:default:=1
	// +kubebuilder:validation:Minimum=1
	NumOfHosts int32 `json:"numOfHosts,omitempty"`
	// DeleteWholeReplicas has KubeRay delete a replica's other pods along with each one Ray
	// gives up, as TPU multi-host slices need.
	// +optional
	DeleteWholeReplicas bool `json:"deleteWholeReplicas,omitempty"`
	// Resources overrides the Ray resources the pods' ray start detects.
	// +optional
	Resources map[string]string `json:"resources,omitempty"`
	// Labels are the Ray labels of the group's nodes.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
	// RayStartParams are the pods' ray start parameters.
	// +optional
	RayStartParams map[string]string `json:"rayStartParams,omitempty"`
	// Template is the pods' template.
	Template corev1.PodTemplateSpec `json:"template"`
}

// RayManagedClusterStatus is KubeRay's status, with Ray's requests and KubeRay's answer.
type RayManagedClusterStatus struct {
	// Requests are Ray's: the replicas it asks for and the pods it gives up. Only Ray writes them,
	// except for the initial replicas KubeRay writes when the cluster is created.
	// +optional
	Requests *RayManagedClusterRequests `json:"requests,omitempty"`
	// Provisioning is KubeRay's answer to Ray's requests.
	// +optional
	Provisioning *RayManagedClusterProvisioning `json:"provisioning,omitempty"`
	// Head is the head pod's and service's addresses.
	// +optional
	Head HeadInfo `json:"head,omitempty"`
	// ObservedGeneration is the last generation KubeRay acted on.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`
	// Conditions: Ended once the cluster's head has stopped.
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

const (
	// RayManagedClusterEnded: the cluster's head stopped (it failed, exited or was deleted). Ray
	// can't restart its head in place yet, so the cluster ends with it: its workers are deleted,
	// and nothing is made again. Recreate the object to start over.
	RayManagedClusterEnded = "Ended"
	// The Ended condition's reasons.
	RayManagedHeadPodFailed    = "HeadPodFailed"
	RayManagedHeadPodSucceeded = "HeadPodSucceeded"
	RayManagedHeadPodDeleted   = "HeadPodDeleted"
)

// RayManagedClusterRequests are Ray's requests, by group.
type RayManagedClusterRequests struct {
	// +optional
	Groups []RayManagedGroupRequests `json:"groups,omitempty"`
}

// RayManagedGroupRequests are Ray's requests of one group.
type RayManagedGroupRequests struct {
	Name string `json:"name"`
	// NewReplicas are the replicas Ray asked for that KubeRay hasn't made, with their instances' IDs.
	// +optional
	NewReplicas []RayManagedNewReplica `json:"newReplicas,omitempty"`
	// DeletePods are the pods Ray gave up, by replica and host, until KubeRay deletes them.
	// +optional
	DeletePods []RayManagedDeletePods `json:"deletePods,omitempty"`
}

// RayManagedNewReplica is a replica Ray asks for.
type RayManagedNewReplica struct {
	// Replica is its number in its group, from 1.
	Replica int64 `json:"replica"`
	// Nodes are its instances' Ray node IDs, in hex, by host.
	Nodes []string `json:"nodes"`
}

// RayManagedDeletePods are pods of a replica Ray gave up.
type RayManagedDeletePods struct {
	Replica int64   `json:"replica"`
	Hosts   []int64 `json:"hosts"`
}

// RayManagedClusterProvisioning is KubeRay's answer to Ray's requests, by group.
type RayManagedClusterProvisioning struct {
	// +optional
	Groups []RayManagedGroupProvisioning `json:"groups,omitempty"`
}

// RayManagedGroupProvisioning is KubeRay's answer for one group.
type RayManagedGroupProvisioning struct {
	Name string `json:"name"`
	// CreatedReplicas is the highest replica KubeRay has handled. It's never lowered.
	CreatedReplicas int64 `json:"createdReplicas"`
	// FailedReplicas are replicas KubeRay couldn't create, until Ray drops them from newReplicas.
	// +optional
	FailedReplicas []RayManagedFailedReplica `json:"failedReplicas,omitempty"`
}

// RayManagedFailedReplica is a replica KubeRay couldn't create.
type RayManagedFailedReplica struct {
	Replica int64  `json:"replica"`
	Message string `json:"message"`
}

// RayManagedCluster is a Ray cluster whose worker pods Ray asks for, through ray.cluster.
// +kubebuilder:object:root=true
// +kubebuilder:resource:categories=all
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="ended",type="string",JSONPath=".status.conditions[?(@.type=='Ended')].reason",priority=0
// +kubebuilder:printcolumn:name="age",type="date",JSONPath=".metadata.creationTimestamp",priority=0
// +kubebuilder:printcolumn:name="head pod IP",type="string",JSONPath=".status.head.podIP",priority=1
type RayManagedCluster struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec RayManagedClusterSpec `json:"spec,omitempty"`
	// +optional
	Status RayManagedClusterStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// RayManagedClusterList contains a list of RayManagedCluster.
type RayManagedClusterList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RayManagedCluster `json:"items"`
}
