import { RayClusterSpec, RayClusterStatus } from "@/types/v2/raycluster";
import { V1ObjectMeta } from "@kubernetes/client-node";

export interface RayClusterListResponse {
  apiVersion: string;
  items: RayClusterItem[];
  kind: string;
  metadata: {
    continue: string;
    resourceVersion: string;
  };
}

interface RayClusterItem {
  apiVersion: string;
  kind: string;
  metadata: V1ObjectMeta;
  spec: RayClusterSpec;
  status: RayClusterStatus;
}
