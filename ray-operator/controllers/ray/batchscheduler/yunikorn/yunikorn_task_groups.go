package yunikorn

import (
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/utils/ptr"

	v1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/common"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

// TaskGroups is a list of task Groups recognized as gang Groups
type TaskGroups struct {
	Groups []TaskGroup `json:"groups"`
}

// TaskGroup is the struct for yunikorn to consider a pod belongs to a gang group
// the original schema is defined here: https://github.com/apache/yunikorn-k8shim/blob/master/pkg/cache/amprotocol.go
type TaskGroup struct {
	MinResource               map[string]resource.Quantity      `json:"minResource"`
	NodeSelector              map[string]string                 `json:"nodeSelector,omitempty"`
	Affinity                  *corev1.Affinity                  `json:"affinity,omitempty"`
	Name                      string                            `json:"name"`
	TopologySpreadConstraints []corev1.TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty"`
	Tolerations               []corev1.Toleration               `json:"tolerations,omitempty"`
	MinMember                 int32                             `json:"minMember"`
}

func newTaskGroups() *TaskGroups {
	return &TaskGroups{
		Groups: make([]TaskGroup, 0),
	}
}

func newTaskGroupsFromRayClusterSpec(rayClusterSpec *v1.RayClusterSpec) *TaskGroups {
	taskGroups := newTaskGroups()

	// head group
	headGroupSpec := rayClusterSpec.HeadGroupSpec
	headPodMinResource := utils.CalculatePodResource(headGroupSpec.Template.Spec)
	taskGroups.addTaskGroup(
		TaskGroup{
			Name:         utils.RayNodeHeadGroupLabelValue,
			MinMember:    1,
			MinResource:  utils.ConvertResourceListToMapString(headPodMinResource),
			NodeSelector: headGroupSpec.Template.Spec.NodeSelector,
			Tolerations:  headGroupSpec.Template.Spec.Tolerations,
			Affinity:     headGroupSpec.Template.Spec.Affinity,
		})

	// worker groups
	for _, workerGroupSpec := range rayClusterSpec.WorkerGroupSpecs {
		workerMinResource := utils.CalculatePodResource(workerGroupSpec.Template.Spec)
		minReplicas := ptr.Deref(workerGroupSpec.MinReplicas, int32(0))
		minWorkers := minReplicas * workerGroupSpec.NumOfHosts
		taskGroups.addTaskGroup(
			TaskGroup{
				Name:         workerGroupSpec.GroupName,
				MinMember:    minWorkers,
				MinResource:  utils.ConvertResourceListToMapString(workerMinResource),
				NodeSelector: workerGroupSpec.Template.Spec.NodeSelector,
				Tolerations:  workerGroupSpec.Template.Spec.Tolerations,
				Affinity:     workerGroupSpec.Template.Spec.Affinity,
			})
	}

	return taskGroups
}

func newTaskGroupsFromRayJobSpec(rayJobSpec *v1.RayJobSpec) *TaskGroups {
	taskGroups := newTaskGroupsFromRayClusterSpec(rayJobSpec.RayClusterSpec)

	submitterGroupSpec := common.GetSubmitterTemplate(rayJobSpec, rayJobSpec.RayClusterSpec).Spec

	submitterPodMinResource := utils.CalculatePodResource(submitterGroupSpec)
	taskGroups.addTaskGroup(
		TaskGroup{
			Name:         utils.RayNodeSubmitterGroupLabelValue,
			MinMember:    1,
			MinResource:  utils.ConvertResourceListToMapString(submitterPodMinResource),
			NodeSelector: submitterGroupSpec.NodeSelector,
			Tolerations:  submitterGroupSpec.Tolerations,
			Affinity:     submitterGroupSpec.Affinity,
		})
	return taskGroups
}

func (t *TaskGroups) addTaskGroup(taskGroup TaskGroup) {
	t.Groups = append(t.Groups, taskGroup)
}

func (t *TaskGroups) marshal() (string, error) {
	result, err := json.Marshal(t.Groups)
	if err != nil {
		return "", err
	}
	return string(result), nil
}

func (t *TaskGroups) unmarshalFrom(spec string) error {
	return json.Unmarshal([]byte(spec), &t.Groups)
}

func (t *TaskGroups) getTaskGroup(name string) TaskGroup {
	for _, group := range t.Groups {
		if group.Name == name {
			return group
		}
	}
	return TaskGroup{}
}
