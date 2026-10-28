package run

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	clusterKinds = "namespaces,clusterroles,clusterrolebindings,customresourcedefinitions,mutatingwebhookconfigurations,validatingwebhookconfigurations"
	// Units create namespaced objects in default or in a namespace of their own, which goes as a
	// whole.
	defaultKinds = "pods,services,configmaps,secrets,serviceaccounts,persistentvolumeclaims,deployments,statefulsets,daemonsets,jobs,cronjobs,ingresses,roles,rolebindings"
)

// snapshot lists what a unit could leave behind. An error means the list is unknown, and sweep
// must not run on it: an empty list would make everything in the cluster look new.
func (r *runner) snapshot(ctx context.Context) ([]string, error) {
	out, err := r.output(ctx, "kubectl", "get", clusterKinds, "-o", "name")
	if err != nil {
		return nil, fmt.Errorf("kubectl get: %w", err)
	}
	objs := strings.Fields(out)
	if kinds := rayCRDs(objs); len(kinds) > 0 {
		items, err := r.list(ctx, strings.Join(kinds, ","), "-A")
		if err != nil {
			return nil, fmt.Errorf("kubectl get ray resources: %w", err)
		}
		for _, item := range items {
			objs = append(objs, item.Kind+"/"+item.Namespace+"/"+item.Name)
		}
	}
	items, err := r.list(ctx, defaultKinds, "-n", "default")
	if err != nil {
		return nil, fmt.Errorf("kubectl get -n default: %w", err)
	}
	for _, item := range items {
		// An object with an owner goes with its owner.
		if len(item.OwnerReferences) == 0 {
			objs = append(objs, "default/"+item.Kind+"/"+item.Name)
		}
	}
	out, err = r.output(ctx, "helm", "list", "-A", "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("helm list: %w", err)
	}
	var releases []struct{ Name, Namespace string }
	if err := json.Unmarshal([]byte(out), &releases); err != nil {
		return nil, fmt.Errorf("helm list: %w", err)
	}
	for _, rel := range releases {
		objs = append(objs, "helm/"+rel.Namespace+"/"+rel.Name)
	}
	return objs, nil
}

// list runs kubectl get -o json and keeps the metadata of each object.
func (r *runner) list(ctx context.Context, kinds string, args ...string) ([]metav1.PartialObjectMetadata, error) {
	out, err := r.output(ctx, "kubectl", append([]string{"get", kinds, "-o", "json"}, args...)...)
	if err != nil {
		return nil, err
	}
	var list metav1.PartialObjectMetadataList
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// sweep deletes what the unit created and its cleanup left, and returns it for the report.
func (r *runner) sweep(ctx context.Context, before []string, log *os.File) []string {
	after, err := r.snapshot(ctx)
	if err != nil {
		fmt.Fprintf(log, "\nsweep skipped: %v\n", err)
		return nil
	}
	left := slices.DeleteFunc(after, func(obj string) bool {
		return slices.Contains(before, obj)
	})
	slices.Sort(left)
	var rest []string
	// Ray resources first, while the operator that removes their finalizers still runs.
	for _, obj := range left {
		if kind, namespace, name, ok := splitRayObject(obj); ok {
			r.toFile(ctx, log, "kubectl", "delete", strings.ToLower(kind)+".ray.io", name, "-n", namespace, "--timeout=60s")
		}
	}
	for _, obj := range left {
		if rel, ok := strings.CutPrefix(obj, "helm/"); ok {
			namespace, name, _ := strings.Cut(rel, "/")
			r.toFile(ctx, log, "helm", "uninstall", name, "-n", namespace, "--wait", "--timeout", "2m")
		} else if object, ok := strings.CutPrefix(obj, "default/"); ok {
			// --ignore-not-found: a Helm release in default may have taken it already.
			r.toFile(ctx, log, "kubectl", "delete", object, "-n", "default", "--ignore-not-found", "--timeout=60s")
		} else if _, _, _, ray := splitRayObject(obj); !ray {
			rest = append(rest, obj)
		}
	}
	if len(rest) > 0 {
		// Wait, so the next unit can create a namespace of the same name.
		r.toFile(ctx, log, "kubectl", append([]string{"delete", "--timeout=120s"}, rest...)...)
	}
	return left
}

// rayCRDs picks the Ray CRDs out of `kubectl get -o name` output. Ray resources can be listed
// only while their CRDs are installed, and one missing kind fails the whole get.
func rayCRDs(objs []string) []string {
	var kinds []string
	for _, obj := range objs {
		if name, ok := strings.CutPrefix(obj, "customresourcedefinition.apiextensions.k8s.io/"); ok && strings.HasSuffix(name, ".ray.io") {
			kinds = append(kinds, name)
		}
	}
	return kinds
}

// splitRayObject splits "RayCluster/<namespace>/<name>".
func splitRayObject(obj string) (kind, namespace, name string, ok bool) {
	parts := strings.Split(obj, "/")
	if len(parts) != 3 || !strings.HasPrefix(parts[0], "Ray") {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

func (r *runner) evidence(ctx context.Context, dir string) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return
	}
	save := func(name string, args ...string) {
		r.toPath(ctx, filepath.Join(dir, name), "kubectl", args...)
	}

	save("pods.txt", "get", "pods", "-A", "-o", "wide")
	save("events.txt", "get", "events", "-A", "--sort-by=.lastTimestamp")

	crds, _ := r.output(ctx, "kubectl", "get", "customresourcedefinitions", "-o", "name")

	if kinds := rayCRDs(strings.Fields(crds)); len(kinds) > 0 {
		// A step that fails right after creating the CRDs can leave some not served yet.
		_, _ = r.output(ctx, "kubectl", append([]string{"wait", "crd", "--for=condition=Established", "--timeout=30s"}, kinds...)...)
		save("ray-resources.yaml", "get", strings.Join(kinds, ","), "-A", "-o", "yaml")
	}

	pods, _ := r.list(ctx, "pods", "-A")
	var described []string
	for _, pod := range pods {
		if strings.HasPrefix(pod.Namespace, "kube-") || pod.Namespace == "local-path-storage" {
			continue
		}
		if !slices.Contains(described, pod.Namespace) {
			described = append(described, pod.Namespace)
			save("describe-pods.txt", "describe", "pods", "-n", pod.Namespace)
		}
		save("pod-logs.txt", "logs", "-n", pod.Namespace, pod.Name, "--all-containers", "--prefix", "--tail=500")
	}
}
