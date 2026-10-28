package types

// For runtime environment proto definitions, please refer to:
// https://github.com/ray-project/ray/blob/91dbbcfb37522e86e8fa24003c9bb3e0d110863b/src/ray/protobuf/public/runtime_environment.proto.

type RuntimeEnvUris struct {
	// Working dir uri.
	WorkingDirURI string `json:"workingDirURI"`
	// Python modules uris.
	PyModulesURIs []string `json:"pyModulesURIs"`
}

type RuntimeEnvConfig struct {
	// The timeout of runtime env creation.
	SetupTimeoutSeconds int `json:"setupTimeoutSeconds"`
	// Indicates whether to install runtime env eagerly before the workers are leased.
	EagerInstall bool `json:"eagerInstall"`
	// A list of files to stream the runtime env setup logs to.
	LogFiles []string `json:"logFiles"`
}

// RuntimeEnvInfo is the runtime environment information transferred between Ray core processes.
type RuntimeEnvInfo struct {
	// The serialized runtime env passed from the user.
	SerializedRuntimeEnv string `json:"serializedRuntimeEnv"`
	// URIs used in this runtime env. These will be used for reference counting.
	Uris *RuntimeEnvUris `json:"uris,omitempty"`
	// The serialized runtime env config passed from the user.
	RuntimeEnvConfig RuntimeEnvConfig `json:"runtimeEnvConfig"`
}
