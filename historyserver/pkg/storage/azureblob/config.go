package azureblob

import (
	"os"
	"strings"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
)

const DefaultContainerName = "ray-historyserver"

// AuthMode specifies the authentication method for Azure Blob Storage
type AuthMode string

const (
	// AuthModeConnectionString uses a connection string for authentication
	AuthModeConnectionString AuthMode = "connection_string"
	// AuthModeWorkloadIdentity uses Azure Workload Identity (for AKS)
	AuthModeWorkloadIdentity AuthMode = "workload_identity"
	// AuthModeDefault uses DefaultAzureCredential which tries multiple methods
	AuthModeDefault AuthMode = "default"
)

type config struct {
	AuthMode         AuthMode
	ConnectionString string
	AccountURL       string
	ContainerName    string
	types.RayCollectorConfig
}

func getContainerNameWithDefault() string {
	container := os.Getenv("AZURE_STORAGE_CONTAINER")
	if container == "" {
		return DefaultContainerName
	}
	return container
}

// parseAuthMode normalizes and validates an auth mode string.
// Returns empty string for auto-detection if invalid.
func parseAuthMode(mode string) AuthMode {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "connection_string":
		return AuthModeConnectionString
	case "workload_identity":
		return AuthModeWorkloadIdentity
	case "default":
		return AuthModeDefault
	default:
		// Auto-detect based on available credentials
		return ""
	}
}

func getAuthMode() AuthMode {
	return parseAuthMode(os.Getenv("AZURE_STORAGE_AUTH_MODE"))
}

func (c *config) populateFromEnvAndJSON(jd map[string]any) {
	c.ConnectionString = os.Getenv("AZURE_STORAGE_CONNECTION_STRING")
	c.AccountURL = os.Getenv("AZURE_STORAGE_ACCOUNT_URL")
	c.ContainerName = getContainerNameWithDefault()
	c.AuthMode = getAuthMode()

	if len(jd) > 0 {
		if v, ok := jd["azureContainer"]; ok {
			if s, ok := v.(string); ok {
				c.ContainerName = s
			}
		}
		if v, ok := jd["azureConnectionString"]; ok {
			if s, ok := v.(string); ok {
				c.ConnectionString = s
			}
		}
		if v, ok := jd["azureAccountURL"]; ok {
			if s, ok := v.(string); ok {
				c.AccountURL = s
			}
		}
		if v, ok := jd["azureAuthMode"]; ok {
			if s, ok := v.(string); ok {
				c.AuthMode = parseAuthMode(s)
			}
		}
	}
}

func (c *config) complete(rcc *types.RayCollectorConfig, jd map[string]any) {
	c.RayCollectorConfig = *rcc
	c.populateFromEnvAndJSON(jd)
}

func (c *config) completeHSConfig(rcc *types.RayHistoryServerConfig, jd map[string]any) {
	c.RayCollectorConfig = types.RayCollectorConfig{RootDir: rcc.RootDir}
	c.populateFromEnvAndJSON(jd)
}
