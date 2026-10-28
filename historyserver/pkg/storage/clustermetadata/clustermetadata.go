package clustermetadata

import (
	"fmt"
	"path"
	"strings"

	"github.com/ray-project/kuberay/historyserver/pkg/utils"
)

const (
	ClusterMetadataDir = "cluster-metadata"
	// Without owner: cluster-metadata/raycluster/{namespace}_{cluster_name}/{session_id}
	metaPartsCountWithoutOwner = 2
	// With owner:    cluster-metadata/{rayjob|rayservice}/{namespace}_{owner_name}_{cluster_name}/{session_id}
	metaPartsCountWithOwner = 3
)

// EncodePath builds the hierarchical path for a given session ID.
func EncodePath(info utils.ClusterInfo, rootDir, sessionID string) string {
	prefix := Prefix(rootDir)
	ownerKind := strings.ToLower(info.OwnerKind)
	hasOwner := (ownerKind == utils.RayJobKind || ownerKind == utils.RayServiceKind) && info.OwnerName != ""

	resourceSubDir := utils.RayClusterKind
	if hasOwner {
		resourceSubDir = ownerKind
	}

	var parts []string
	parts = append(parts, info.Namespace)
	if hasOwner {
		parts = append(parts, info.OwnerName)
	}
	parts = append(parts, info.Name)
	p := path.Join(prefix, resourceSubDir, strings.Join(parts, utils.Connector))
	return path.Clean(path.Join(p, path.Base(sessionID)))
}

// DecodePath parses the hierarchical path and returns the cluster info.
func DecodePath(filePath string, rootDir string) (utils.ClusterInfo, error) {
	prefix := Prefix(rootDir)
	cleanFilePath := strings.Trim(filePath, "/")
	cleanPrefix := strings.Trim(prefix, "/") + "/"
	if !strings.HasPrefix(cleanFilePath, cleanPrefix) {
		return utils.ClusterInfo{}, fmt.Errorf("invalid path %q: expected prefix %q", filePath, prefix)
	}
	relativePath := strings.Trim(strings.TrimPrefix(cleanFilePath, cleanPrefix), "/")

	pathSegments := strings.Split(relativePath, "/")

	if len(pathSegments) != 3 {
		return utils.ClusterInfo{}, fmt.Errorf("invalid path segment count for cluster metadata structure: %s", filePath)
	}

	resourceSubDir := strings.ToLower(pathSegments[0])
	if resourceSubDir != utils.RayClusterKind && resourceSubDir != utils.RayJobKind && resourceSubDir != utils.RayServiceKind {
		return utils.ClusterInfo{}, fmt.Errorf("unsupported cluster metadata subdirectory %q in path %q", resourceSubDir, filePath)
	}

	c := utils.ClusterInfo{
		SessionName: pathSegments[2],
	}

	metaParts := strings.Split(pathSegments[1], utils.Connector)
	c.Namespace = metaParts[0]
	switch len(metaParts) {
	case metaPartsCountWithoutOwner:
		if resourceSubDir != utils.RayClusterKind {
			return utils.ClusterInfo{}, fmt.Errorf("mismatched subdirectory %q for metadata structure without owner in path %q", resourceSubDir, filePath)
		}
		c.Name = metaParts[1]
	case metaPartsCountWithOwner:
		if resourceSubDir != utils.RayJobKind && resourceSubDir != utils.RayServiceKind {
			return utils.ClusterInfo{}, fmt.Errorf("unsupported subdirectory %q with owner metadata in path %q", resourceSubDir, filePath)
		}
		c.OwnerKind = resourceSubDir
		c.OwnerName = metaParts[1]
		c.Name = metaParts[2]
	default:
		return utils.ClusterInfo{}, fmt.Errorf("invalid cluster-metadata segment count %d in path %q", len(metaParts), filePath)
	}

	createTime, err := utils.GetDateTimeFromSessionID(c.SessionName)
	if err != nil {
		return utils.ClusterInfo{}, fmt.Errorf("parse session time from %s: %w", c.SessionName, err)
	}
	c.CreateTimeStamp = createTime.Unix()
	c.CreateTime = createTime.UTC().Format("2006-01-02T15:04:05Z")

	return c, nil
}

// Prefix returns the root directory prefix.
func Prefix(rootDir string) string {
	if rootDir == "" {
		return ClusterMetadataDir + "/"
	}
	return strings.TrimRight(path.Join(rootDir, ClusterMetadataDir), "/") + "/"
}
