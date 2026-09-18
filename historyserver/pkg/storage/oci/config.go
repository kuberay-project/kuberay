package oci

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/types"
)

const (
	// DefaultBucket is used when neither OCI_BUCKET nor the "ociBucket" JSON key is set.
	DefaultBucket = "ray-historyserver"

	// BucketEnvVar names the Object Storage bucket.
	BucketEnvVar = "OCI_BUCKET"
	// NamespaceEnvVar is the Object Storage namespace. Resolved from the tenancy when empty.
	NamespaceEnvVar = "OCI_NAMESPACE"
	// RegionEnvVar overrides the region taken from the credentials (e.g. "us-ashburn-1").
	RegionEnvVar = "OCI_REGION"
	// CompartmentIDEnvVar is the compartment used to create the bucket when it does not exist.
	CompartmentIDEnvVar = "OCI_COMPARTMENT_ID"
	// AuthTypeEnvVar selects the credential source; see AuthType. Auto-detected when empty.
	AuthTypeEnvVar = "OCI_AUTH_TYPE"
	// ConfigFileEnvVar points at an OCI SDK config file (default ~/.oci/config).
	ConfigFileEnvVar = "OCI_CONFIG_FILE"
	// ConfigProfileEnvVar is the profile inside the config file (default "DEFAULT").
	ConfigProfileEnvVar = "OCI_CONFIG_PROFILE"

	// Set by OKE Workload Identity and other resource principal environments.
	resourcePrincipalVersionEnvVar = "OCI_RESOURCE_PRINCIPAL_VERSION"
	// Set by Kubernetes in every pod; distinguishes OKE Workload Identity from other resource principals.
	kubernetesServiceHostEnvVar = "KUBERNETES_SERVICE_HOST"

	defaultConfigProfile = "DEFAULT"
	defaultConfigDir     = ".oci"
	defaultConfigFile    = "config"
)

// AuthType selects how the OCI SDK authenticates against Object Storage.
type AuthType string

const (
	// AuthTypeAPIKey uses an API signing key from an OCI config file (or OCI_* env vars).
	AuthTypeAPIKey AuthType = "api_key"
	// AuthTypeSessionToken uses a profile created by `oci session authenticate`.
	AuthTypeSessionToken AuthType = "session_token"
	// AuthTypeInstancePrincipal uses the identity of the OCI compute instance.
	AuthTypeInstancePrincipal AuthType = "instance_principal"
	// AuthTypeWorkloadIdentity uses OKE Workload Identity (the pod's ServiceAccount).
	AuthTypeWorkloadIdentity AuthType = "oke_workload_identity"
	// AuthTypeResourcePrincipal uses a generic OCI resource principal (e.g. OCI Functions).
	AuthTypeResourcePrincipal AuthType = "resource_principal"
)

type config struct {
	// AuthType is kept as the raw string; New resolves and validates it.
	AuthType      string
	Bucket        string
	Namespace     string
	Region        string
	CompartmentID string
	ConfigFile    string
	ConfigProfile string
	types.RayCollectorConfig
}

func getBucketWithDefault() string {
	bucket := os.Getenv(BucketEnvVar)
	if bucket == "" {
		return DefaultBucket
	}
	return bucket
}

// parseAuthType normalizes an auth type string. An empty string means auto-detect.
func parseAuthType(raw string) (AuthType, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "":
		return "", nil
	case "api_key", "apikey", "config_file":
		return AuthTypeAPIKey, nil
	case "session_token", "security_token":
		return AuthTypeSessionToken, nil
	case "instance_principal":
		return AuthTypeInstancePrincipal, nil
	case "oke_workload_identity", "workload_identity":
		return AuthTypeWorkloadIdentity, nil
	case "resource_principal":
		return AuthTypeResourcePrincipal, nil
	default:
		return "", fmt.Errorf("unsupported OCI auth type %q: expected one of %s, %s, %s, %s, %s",
			raw, AuthTypeAPIKey, AuthTypeSessionToken, AuthTypeInstancePrincipal,
			AuthTypeWorkloadIdentity, AuthTypeResourcePrincipal)
	}
}

func setStringFromJSON(jd map[string]any, key string, dst *string) {
	if v, ok := jd[key]; ok {
		if s, ok := v.(string); ok {
			*dst = s
		}
	}
}

// populateFromEnvAndJSON reads the environment first and lets the JSON config
// file (--storage-backend-config-path) override individual keys.
func (c *config) populateFromEnvAndJSON(jd map[string]any) {
	c.Bucket = getBucketWithDefault()
	c.Namespace = os.Getenv(NamespaceEnvVar)
	c.Region = os.Getenv(RegionEnvVar)
	c.CompartmentID = os.Getenv(CompartmentIDEnvVar)
	c.AuthType = os.Getenv(AuthTypeEnvVar)
	c.ConfigFile = os.Getenv(ConfigFileEnvVar)
	c.ConfigProfile = os.Getenv(ConfigProfileEnvVar)

	if len(jd) > 0 {
		setStringFromJSON(jd, "ociBucket", &c.Bucket)
		setStringFromJSON(jd, "ociNamespace", &c.Namespace)
		setStringFromJSON(jd, "ociRegion", &c.Region)
		setStringFromJSON(jd, "ociCompartmentId", &c.CompartmentID)
		setStringFromJSON(jd, "ociAuthType", &c.AuthType)
		setStringFromJSON(jd, "ociConfigFile", &c.ConfigFile)
		setStringFromJSON(jd, "ociConfigProfile", &c.ConfigProfile)
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

// profile returns the config file profile, defaulting to DEFAULT.
func (c *config) profile() string {
	if p := strings.TrimSpace(c.ConfigProfile); p != "" {
		return p
	}
	return defaultConfigProfile
}

// configFilePath returns the OCI config file path this backend will use:
// the explicit setting, then OCI_CONFIG_FILE (also honored by the SDK), then ~/.oci/config.
func (c *config) configFilePath() string {
	if c.ConfigFile != "" {
		return c.ConfigFile
	}
	if p := os.Getenv(ConfigFileEnvVar); p != "" {
		return p
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, defaultConfigDir, defaultConfigFile)
}

// resolveAuthType returns the explicit auth type, or auto-detects one:
//
//  1. OCI_RESOURCE_PRINCIPAL_VERSION is set inside a pod  -> oke_workload_identity
//  2. OCI_RESOURCE_PRINCIPAL_VERSION is set elsewhere      -> resource_principal
//  3. an OCI config file exists                            -> session_token if the profile
//     has a security_token_file, api_key otherwise
//  4. nothing above                                        -> instance_principal
func (c *config) resolveAuthType() (AuthType, error) {
	authType, err := parseAuthType(c.AuthType)
	if err != nil || authType != "" {
		return authType, err
	}

	if os.Getenv(resourcePrincipalVersionEnvVar) != "" {
		if os.Getenv(kubernetesServiceHostEnvVar) != "" {
			return AuthTypeWorkloadIdentity, nil
		}
		return AuthTypeResourcePrincipal, nil
	}

	if p := c.configFilePath(); fileExists(p) {
		if profileHasSessionToken(p, c.profile()) {
			return AuthTypeSessionToken, nil
		}
		return AuthTypeAPIKey, nil
	}

	return AuthTypeInstancePrincipal, nil
}

func fileExists(p string) bool {
	if p == "" {
		return false
	}
	info, err := os.Stat(p)
	return err == nil && !info.IsDir()
}

// profileHasSessionToken reports whether the given profile in an OCI config file
// declares a security_token_file, which marks it as a session token profile.
// Parsing errors are treated as "no session token" so the api_key path can report them.
func profileHasSessionToken(configPath string, profile string) bool {
	f, err := os.Open(configPath)
	if err != nil {
		return false
	}
	defer f.Close()

	inProfile := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, ";") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			inProfile = strings.TrimSpace(line[1:len(line)-1]) == profile
			continue
		}
		if !inProfile {
			continue
		}
		key, value, found := strings.Cut(line, "=")
		if found && strings.TrimSpace(key) == "security_token_file" && strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}
