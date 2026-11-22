# Development

This section walks through how to build and test the plugin.

## Requirements

| software | version  | link                         |
|:---------|:---------|:-----------------------------|
| kubectl  |  >= 1.23 | [download][download-kubectl] |
| go       |  v1.26   | [download-go]                |

## IDE Setup (VS Code)

1. Install the [VS Code Go extension]
1. Import the KubeRay workspace configuration by using the file `kuberay.code-workspace` in the root
   of the KubeRay git repo: "File" -> "Open Workspace from File" -> "kuberay.code-workspace".

Setting up workspace configuration is required because KubeRay contains multiple Go modules. See the
[VS Code Go documentation] for details.

### Run unit tests

Run unit tests with the following command.

```console
cd kubectl-plugin
go test ./pkg/... -race -parallel 4
```

### Manual Tests

To manually test the plugin, you can build and install the plugin locally and run the plugin
commands against a Kubernetes cluster where you have deployed the [KubeRay Operator].

1. Build the plugin with `go build cmd/kubectl-ray.go`.
1. If you manage your plugins with [Krew], move the executable file to the Krew plugin directory with `mv
   kubectl-ray ~/.krew/bin`. Otherwise, move the executable to anywhere in your `PATH`.
1. Run plugin commands against a Kubernetes cluster where you have deployed the [KubeRay Operator].
   Select the cluster by first running `kubectl config use-context <context>` or per-command with
   the `kubectl ray --context` flag.

[download-kubectl]: https://kubernetes.io/docs/tasks/tools/install-kubectl/
[download-go]: https://golang.org/dl/
[VS Code Go extension]: https://marketplace.visualstudio.com/items?itemName=golang.Go
[VS Code Go documentation]: https://github.com/golang/vscode-go/blob/master/README.md#setting-up-your-workspace
[KubeRay Operator]: https://docs.ray.io/en/latest/cluster/kubernetes/index.html
[Krew]: https://krew.sigs.k8s.io/
