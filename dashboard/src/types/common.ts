export interface DeletionStrategy {
  onSuccess: DeletionPolicy;
  onFailure: DeletionPolicy;
}

interface DeletionPolicy {
  policy?: DeletionPolicyType;
}

type DeletionPolicyType =
  | "DeleteCluster"
  | "DeleteWorkers"
  | "DeleteSelf"
  | "DeleteNone";

export interface HeadInfo {
  podIP?: string;
  serviceIP?: string;
  podName?: string;
  serviceName?: string;
}

export interface AutoscalerOptions {
  resources?: {
    limits?: Record<string, string | number>;
    requests?: Record<string, string | number>;
  };
  image?: string;
  imagePullPolicy?: string;
  securityContext?: Record<string, any>;
  idleTimeoutSeconds?: number;
  upscalingMode?: "Default" | "Aggressive" | "Conservative";
  version?: "v1" | "v2";
  env?: Array<Record<string, any>>;
  envFrom?: Array<Record<string, any>>;
  volumeMounts?: Array<Record<string, any>>;
}

export enum ResourceKey {
  GPU = "nvidia.com/gpu",
}
