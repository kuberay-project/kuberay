import { AutoscalerOptions, HeadInfo } from "../common";
import { V1Condition, V1PodTemplateSpec } from "@kubernetes/client-node";
export interface RayClusterSpec {
  suspend?: boolean;
  managedBy?: string;
  autoscalerOptions?: AutoscalerOptions;
  headServiceAnnotations?: Record<string, string>;
  enableInTreeAutoscaling?: boolean;
  gcsFaultToleranceOptions?: GcsFaultToleranceOptions;
  headGroupSpec: HeadGroupSpec;
  rayVersion: string;
  workerGroupSpecs?: WorkerGroupSpec[];
}
interface GcsFaultToleranceOptions {
  redisUsername?: RedisCredential;
  redisPassword?: RedisCredential;
  externalStorageNamespace?: string;
  redisAddress: string;
}
interface RedisCredential {
  valueFrom?: Record<string, any>;
  value?: string;
}

export interface HeadGroupSpec {
  template: V1PodTemplateSpec;
  headService?: any;
  enableIngress?: boolean;
  rayStartParams?: Record<string, string>;
  serviceType?: string;
}

export interface WorkerGroupSpec {
  suspend?: boolean;
  groupName: string;
  replicas?: number;
  minReplicas: number;
  maxReplicas: number;
  idleTimeoutSeconds?: number;
  rayStartParams?: Record<string, string>;
  template: V1PodTemplateSpec;
  scaleStrategy?: ScaleStrategy;
  numOfHosts: number;
}

export enum UpscalingMode {
  DEFAULT = "DEFAULT",
  Conservative = "Conservative",
  AGGRESSIVE = "Aggressive",
}

export enum AutoscalerVersion {
  v1 = "v1",
  v2 = "v2",
}

interface ScaleStrategy {
  workersToDelete: string[];
}

export interface SubmitterConfig {
  backoffLimit: number;
}

export enum ClusterState {
  Ready = "ready",
  Failed = "failed",
  Suspended = "suspended",
}

export interface RayClusterStatus {
  state?: ClusterState;
  desiredCPU?: string;
  desiredMemory?: string;
  desiredGPU?: string;
  desiredTPU?: string;
  lastUpdateTime?: string;
  stateTransitionTimes?: Record<ClusterState, string>;
  endpoints: { [key: string]: string };
  head?: HeadInfo;
  reason?: string;
  conditions?: V1Condition[];
  readyWorkerReplicas?: number;
  availableWorkerReplicas?: number;
  desiredWorkerReplicas?: number;
  minWorkerReplicas?: number;
  maxWorkerReplicas?: number;
  observedGeneration?: number;
}

export enum ClusterStatus {
  SUSPENDED = "SUSPENDED",
  SUSPENDING = "SUSPENDING",
  PENDING = "PENDING",
  READY = "READY",
  FAILED = "FAILED",
}
