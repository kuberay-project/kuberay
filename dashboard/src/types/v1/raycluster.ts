export interface RayStartParams {
  "dashboard-host"?: string;
  "metrics-export-port"?: string;
}

export interface WorkerGroupSpec {
  groupName: string;
  computeTemplate: string;
  image: string;
  replicas: number;
  minReplicas: number;
  maxReplicas: number;
  rayStartParams: RayStartParams;
  labels: { [key: string]: string };
}

export interface HeadGroupSpec {
  computeTemplate: string;
  image: string;
  rayStartParams: RayStartParams;
  labels: { [key: string]: string };
  serviceType?: string;
}

export interface ClusterSpec {
  headGroupSpec: HeadGroupSpec;
  workerGroupSpec: WorkerGroupSpec[];
}

export interface Cluster {
  name: string;
  namespace: string;
  user: string;
  clusterSpec: ClusterSpec;
  createdAt: string;
  clusterState: string;
  annotations: { [key: string]: string };
  version: string;
  rayHeadDashboardLink: string;
  rayGrafanaDashboardLink: string;
  notebookLink: string;
}

export interface ClusterRow {
  name: string;
  clusterState: string;
  createdAt: string;
  links: {
    rayHeadDashboardLink: string;
    rayGrafanaDashboardLink: string;
    notebookLink: string;
  };
}

export type ClusterStatus = "PENDING" | "READY";
