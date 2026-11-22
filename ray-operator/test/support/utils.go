package support

import (
	"context"
	"io/fs"
	"os"
	"path"

	"github.com/stretchr/testify/require"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

//go:fix inline
func Ptr[T any](v T) *T {
	return new(v)
}

type OutputType string

const (
	Log OutputType = "log"
)

func WriteToOutputDir(t Test, fileName string, fileType OutputType, data []byte) {
	t.T().Helper()
	err := os.WriteFile(path.Join(t.OutputDir(), fileName+"."+string(fileType)), data, fs.ModePerm)
	require.NoError(t.T(), err)
}

// EndpointInfo contains information about a ready endpoint from an EndpointSlice.
type EndpointInfo struct {
	TargetRefName string
	TargetRefUID  types.UID
}

// GetReadyEndpointsFromSlices retrieves all ready endpoints from EndpointSlices
// associated with the given service name in the specified namespace.
// It returns a slice of EndpointInfo containing target reference names and UIDs of ready endpoints.
func GetReadyEndpointsFromSlices(ctx context.Context, client Client, namespace, serviceName string) ([]EndpointInfo, error) {
	endpointSliceList, err := client.Core().DiscoveryV1().EndpointSlices(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: discoveryv1.LabelServiceName + "=" + serviceName,
	})
	if err != nil {
		return nil, err
	}

	var readyEndpoints []EndpointInfo
	for _, slice := range endpointSliceList.Items {
		for _, endpoint := range slice.Endpoints {
			if ptr.Deref(endpoint.Conditions.Ready, false) {
				if endpoint.TargetRef != nil && endpoint.TargetRef.UID != "" {
					readyEndpoints = append(readyEndpoints, EndpointInfo{
						TargetRefName: endpoint.TargetRef.Name,
						TargetRefUID:  endpoint.TargetRef.UID,
					})
				}
			}
		}
	}

	return readyEndpoints, nil
}
