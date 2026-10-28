package get

import (
	"context"
	"fmt"

	"github.com/spf13/cobra"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"

	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/client"
	"github.com/ray-project/kuberay/kubectl-plugin/pkg/util/completion"
	rayv1 "github.com/ray-project/kuberay/ray-operator/apis/ray/v1"
	"github.com/ray-project/kuberay/ray-operator/controllers/ray/utils"
)

type GetTokenOptions struct {
	cmdFactory cmdutil.Factory
	ioStreams  *genericclioptions.IOStreams
	namespace  string
	cluster    string
}

func NewGetTokenOptions(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *GetTokenOptions {
	return &GetTokenOptions{
		cmdFactory: cmdFactory,
		ioStreams:  &streams,
	}
}

func NewGetTokenCommand(cmdFactory cmdutil.Factory, streams genericclioptions.IOStreams) *cobra.Command {
	options := NewGetTokenOptions(cmdFactory, streams)

	cmd := &cobra.Command{
		Use:               "token [CLUSTER NAME]",
		Aliases:           []string{"token"},
		Short:             "Get the auth token from the ray cluster.",
		SilenceUsage:      true,
		ValidArgsFunction: completion.RayClusterCompletionFunc(cmdFactory),
		Args:              cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := options.Complete(args); err != nil {
				return err
			}
			// running cmd.Execute or cmd.ExecuteE sets the context, which will be done by root
			k8sClient, err := client.NewClient(cmdFactory)
			if err != nil {
				return fmt.Errorf("failed to create client: %w", err)
			}
			return options.Run(cmd.Context(), k8sClient)
		},
	}
	return cmd
}

func (options *GetTokenOptions) Complete(args []string) error {
	namespace, _, err := options.cmdFactory.ToRawKubeConfigLoader().Namespace()
	if err != nil {
		return fmt.Errorf("failed to get namespace: %w", err)
	}
	options.namespace = namespace
	// guarded by cobra.ExactArgs(1)
	options.cluster = args[0]
	return nil
}

func (options *GetTokenOptions) Run(ctx context.Context, k8sClient client.Client) error {
	cluster, err := k8sClient.RayClient().RayV1().RayClusters(options.namespace).Get(ctx, options.cluster, v1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get RayCluster %s/%s: %w", options.namespace, options.cluster, err)
	}
	if cluster.Spec.AuthOptions == nil || cluster.Spec.AuthOptions.Mode != rayv1.AuthModeToken {
		return fmt.Errorf("RayCluster %s/%s was not configured to use authentication tokens", options.namespace, options.cluster)
	}
	// TODO: support custom token secret?
	secret, err := k8sClient.KubernetesClient().CoreV1().Secrets(options.namespace).Get(ctx, options.cluster, v1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get secret %s/%s: %w", options.namespace, options.cluster, err)
	}
	if token, ok := secret.Data[utils.RAY_AUTH_TOKEN_SECRET_KEY]; ok {
		_, err = fmt.Fprint(options.ioStreams.Out, string(token))
	} else {
		err = fmt.Errorf("secret %s/%s does not have an auth_token", options.namespace, options.cluster)
	}
	return err
}
