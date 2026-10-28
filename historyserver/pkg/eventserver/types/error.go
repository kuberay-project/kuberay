package types

// For error-related proto definitions, please refer to:
// https://github.com/ray-project/ray/blob/d0b1d151d8ea964a711e451d0ae736f8bf95b629/src/ray/protobuf/common.proto#L292-L306.

type ErrorType string

const (
	WorkerDied                                 ErrorType = "WORKER_DIED"
	ActorDied                                  ErrorType = "ACTOR_DIED"
	TaskExecutionException                     ErrorType = "TASK_EXECUTION_EXCEPTION"
	ObjectInPlasma                             ErrorType = "OBJECT_IN_PLASMA"
	TaskCancelled                              ErrorType = "TASK_CANCELLED"
	ActorCreationFailed                        ErrorType = "ACTOR_CREATION_FAILED"
	RuntimeEnvSetupFailed                      ErrorType = "RUNTIME_ENV_SETUP_FAILED"
	ObjectLost                                 ErrorType = "OBJECT_LOST"
	OwnerDied                                  ErrorType = "OWNER_DIED"
	ObjectDeleted                              ErrorType = "OBJECT_DELETED"
	DependencyResolutionFailed                 ErrorType = "DEPENDENCY_RESOLUTION_FAILED"
	ObjectUnreconstructableMaxAttemptsExceeded ErrorType = "OBJECT_UNRECONSTRUCTABLE_MAX_ATTEMPTS_EXCEEDED"
	ObjectUnreconstructableLineageEvicted      ErrorType = "OBJECT_UNRECONSTRUCTABLE_LINEAGE_EVICTED"
	ObjectFetchTimedOut                        ErrorType = "OBJECT_FETCH_TIMED_OUT"
	LocalRayletDied                            ErrorType = "LOCAL_RAYLET_DIED"
	TaskPlacementGroupRemoved                  ErrorType = "TASK_PLACEMENT_GROUP_REMOVED"
	ActorPlacementGroupRemoved                 ErrorType = "ACTOR_PLACEMENT_GROUP_REMOVED"
	TaskUnschedulableError                     ErrorType = "TASK_UNSCHEDULABLE_ERROR"
	ActorUnschedulableError                    ErrorType = "ACTOR_UNSCHEDULABLE_ERROR"
	OutOfDiskError                             ErrorType = "OUT_OF_DISK_ERROR"
	ObjectFreed                                ErrorType = "OBJECT_FREED"
	OutOfMemory                                ErrorType = "OUT_OF_MEMORY"
	NodeDied                                   ErrorType = "NODE_DIED"
	EndOfStreamingGenerator                    ErrorType = "END_OF_STREAMING_GENERATOR"
	ActorUnavailable                           ErrorType = "ACTOR_UNAVAILABLE"
	GeneratorTaskFailedForObjectReconstruction ErrorType = "GENERATOR_TASK_FAILED_FOR_OBJECT_RECONSTRUCTION"
	ObjectUnreconstructablePut                 ErrorType = "OBJECT_UNRECONSTRUCTABLE_PUT"
	ObjectUnreconstructableRetriesDisabled     ErrorType = "OBJECT_UNRECONSTRUCTABLE_RETRIES_DISABLED"
	ObjectUnreconstructableBorrowed            ErrorType = "OBJECT_UNRECONSTRUCTABLE_BORROWED"
	ObjectUnreconstructableLocalMode           ErrorType = "OBJECT_UNRECONSTRUCTABLE_LOCAL_MODE"
	ObjectUnreconstructableRefNotFound         ErrorType = "OBJECT_UNRECONSTRUCTABLE_REF_NOT_FOUND"
	ObjectUnreconstructableTaskCancelled       ErrorType = "OBJECT_UNRECONSTRUCTABLE_TASK_CANCELLED"
	ObjectUnreconstructableLineageDisabled     ErrorType = "OBJECT_UNRECONSTRUCTABLE_LINEAGE_DISABLED"
)

// RayErrorInfo is the information per Ray error type.
type RayErrorInfo struct {
	// More detailed error context for various error types.
	// TODO(jiangjiawei1103): Add error context.
	Error any `json:"error"`

	ErrorMessage string `json:"errorMessage"`

	// The type of error that caused the exception.
	ErrorType ErrorType `json:"errorType"`
}
