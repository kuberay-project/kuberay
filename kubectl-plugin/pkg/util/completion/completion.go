package completion

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/kubectl/pkg/util/completion"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
)

// RayResourceTypeCompletionFunc Returns a completion function that completes the Ray resource type.
// That is, raycluster, rayjob, or rayservice.
func RayResourceTypeCompletionFunc() func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		var comps []string
		directive := cobra.ShellCompDirectiveNoFileComp
		resourceTypes := getAllRayResourceType()
		for _, resourceType := range resourceTypes {
			if strings.HasPrefix(resourceType, toComplete) {
				comps = append(comps, resourceType)
			}
		}
		return comps, directive
	}
}

// RayClusterCompletionFunc Returns a completion function that completes RayCluster resource names.
func RayClusterCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return completion.ResourceNameCompletionFunc(f, string(util.RayCluster))
}

// RayJobCompletionFunc Returns a completion function that completes RayJob resource names.
func RayJobCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return completion.ResourceNameCompletionFunc(f, string(util.RayJob))
}

// RayServiceCompletionFunc Returns a completion function that completes RayService resource names.
func RayServiceCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return completion.ResourceNameCompletionFunc(f, string(util.RayService))
}

// RayClusterResourceNameCompletionFunc Returns completions of:
// 1- RayCluster names that match the toComplete prefix
// 2- Ray resource types which match the toComplete prefix
func RayClusterResourceNameCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		var comps []string
		directive := cobra.ShellCompDirectiveNoFileComp
		if len(args) == 0 {
			comps, directive = doRayClusterCompletion(f, toComplete)
		}
		return comps, directive
	}
}

// WorkerGroupCompletionFunc returns a completion function that completes WorkerGroup resource names.
func WorkerGroupCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		k8sClient, err := client.NewClient(f)
		if err != nil {
			return []string{}, cobra.ShellCompDirectiveNoFileComp
		}

		return workerGroupCompletionFunc(cmd, args, toComplete, k8sClient, f)
	}
}

// workerGroupCompletionFunc Returns completions of:
// Workergroup names that match the toComplete prefix and respect flag values (--ray-cluster, --namespace, --all-namespaces)
func workerGroupCompletionFunc(cmd *cobra.Command, args []string, toComplete string, k8sClient client.Client, f cmdutil.Factory) ([]string, cobra.ShellCompDirective) {
	var comps []string
	directive := cobra.ShellCompDirectiveNoFileComp

	// Stop completion after first argument to prevent multiple completions
	if len(args) != 0 {
		return comps, directive
	}

	cluster, _ := cmd.Flags().GetString("ray-cluster")
	namespace, _, err := f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return comps, directive
	}
	allNamespaces, _ := cmd.Flags().GetBool("all-namespaces")

	if allNamespaces {
		return comps, directive
	}

	listopts := v1.ListOptions{}
	if cluster != "" {
		listopts = v1.ListOptions{
			FieldSelector: fmt.Sprintf("metadata.name=%s", cluster),
		}
	}

	rayClusterList, err := k8sClient.RayClient().RayV1().RayClusters(namespace).List(context.Background(), listopts)
	if err != nil {
		return comps, directive
	}

	seen := make(map[string]bool)
	for _, rayCluster := range rayClusterList.Items {
		for _, spec := range rayCluster.Spec.WorkerGroupSpecs {
			if !seen[spec.GroupName] && (toComplete == "" || strings.HasPrefix(spec.GroupName, toComplete)) {
				comps = append(comps, spec.GroupName)
				seen[spec.GroupName] = true
			}
		}
	}
	return comps, directive
}

// NodeCompletionFunc returns a completion function that completes Node resource names.
func NodeCompletionFunc(f cmdutil.Factory) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		k8sClient, err := client.NewClient(f)
		if err != nil {
			return []string{}, cobra.ShellCompDirectiveNoFileComp
		}

		return nodeCompletionFunc(cmd, args, toComplete, k8sClient, f)
	}
}

// nodeCompletionFunc Returns completions of:
// Node names that match the toComplete prefix and respect flag values (--ray-cluster, --namespace)
func nodeCompletionFunc(cmd *cobra.Command, args []string, toComplete string, k8sClient client.Client, f cmdutil.Factory) ([]string, cobra.ShellCompDirective) {
	var comps []string
	directive := cobra.ShellCompDirectiveNoFileComp

	// Stop completion after first argument to prevent multiple completions
	if len(args) != 0 {
		return comps, directive
	}

	cluster, _ := cmd.Flags().GetString("ray-cluster")
	namespace, _, err := f.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return comps, directive
	}
	allNamespaces, _ := cmd.Flags().GetBool("all-namespaces")

	if allNamespaces {
		return comps, directive
	}

	labelSelectors := createRayNodeLabelSelectors(cluster)
	pods, err := k8sClient.KubernetesClient().CoreV1().Pods(namespace).List(
		context.Background(),
		v1.ListOptions{
			LabelSelector: labels.Set(labelSelectors).String(),
		},
	)
	if err != nil {
		return comps, directive
	}

	for _, pod := range pods.Items {
		if toComplete == "" || strings.HasPrefix(pod.Name, toComplete) {
			comps = append(comps, pod.Name)
		}
	}
	return comps, directive
}

// createRayNodeLabelSelectors creates a map of K8s label selectors for Ray nodes
// TODO: duplicated function as in kubectl/pkg/cmd/get/get_nodes.go
func createRayNodeLabelSelectors(cluster string) map[string]string {
	labelSelectors := map[string]string{
		util.RayIsRayNodeLabelKey: "yes",
	}
	if cluster != "" {
		labelSelectors[util.RayClusterLabelKey] = cluster
	}
	return labelSelectors
}

func getAllRayResourceType() []string {
	return []string{
		string(util.RayCluster),
		string(util.RayJob),
		string(util.RayService),
	}
}

// doRayClusterCompletion Returns completions of:
// 1- RayCluster names that match the toComplete prefix
// 2- Ray resource types which match the toComplete prefix
// Ref: https://github.com/kubernetes/kubectl/blob/262825a8a665c7cae467dfaa42b63be5a5b8e5a2/pkg/util/completion/completion.go#L434
func doRayClusterCompletion(f cmdutil.Factory, toComplete string) ([]string, cobra.ShellCompDirective) {
	var comps []string
	directive := cobra.ShellCompDirectiveNoFileComp
	slashIdx := strings.Index(toComplete, "/")
	if slashIdx == -1 {
		// Standard case, complete RayCluster names
		comps = completion.CompGetResource(f, string(util.RayCluster), toComplete)

		// Also include resource choices for the <type>/<name> form
		resourceTypes := getAllRayResourceType()

		if len(comps) == 0 {
			// If there are no RayCluster to complete, we will only be completing
			// <type>/.  We should disable adding a space after the /.
			directive |= cobra.ShellCompDirectiveNoSpace
		}

		for _, resource := range resourceTypes {
			if strings.HasPrefix(resource, toComplete) {
				comps = append(comps, fmt.Sprintf("%s/", resource))
			}
		}
	} else {
		// Dealing with the <type>/<name> form, use the specified resource type
		resourceType := toComplete[:slashIdx]
		toComplete = toComplete[slashIdx+1:]
		nameComps := completion.CompGetResource(f, resourceType, toComplete)
		for _, c := range nameComps {
			comps = append(comps, fmt.Sprintf("%s/%s", resourceType, c))
		}
	}
	return comps, directive
}
