import { Cluster, ClusterRow, ClusterStatus } from "@/types/v1/raycluster";
import { Job, JobRow, Jobs, Status } from "@/types/v1/rayjob";

export const filterJobs = (
  jobs: Jobs,
  search: string,
  statusFilter: Status | null,
  typeFilter: number,
): JobRow[] =>
  jobs
    .map((job) => {
      if (!job.jobStatus) {
        job.jobStatus = "PENDING";
      }
      return job;
    })
    .filter((job: Job) => {
      // case-insensitive search
      if (
        statusFilter &&
        job.jobStatus.toUpperCase() !== statusFilter.toUpperCase()
      ) {
        return false;
      }
      if (search && !job.name.toUpperCase().includes(search.toUpperCase())) {
        return false;
      }
      if (
        typeFilter == 1 &&
        job.clusterSpec?.headGroupSpec?.labels?.["mlp.rbx.com/component"] !==
          "rayllmbatchinference"
      ) {
        return false;
      }
      return true;
    })
    .map(transformJob);

const transformJob = (job: Job): JobRow => {
  return {
    name: job.name,
    jobStatus: {
      jobStatus: job.jobStatus,
      jobDeploymentStatus: job.jobDeploymentStatus || "",
    },
    createdAt: job.createdAt,
    links: {
      rayGrafanaDashboardLink: job.rayGrafanaDashboardLink,
      logsLink: job.logsLink,
      rayHeadDashboardLink: job.rayHeadDashboardLink,
    },
    message: job.message || "",
  };
};

export const filterCluster = (
  clusters: Cluster[],
  search: string,
  statusFilter: ClusterStatus | null,
  typeFilter: number,
): ClusterRow[] =>
  clusters
    .map((cluster) => {
      if (!cluster.clusterState) {
        cluster.clusterState = "PENDING";
      }
      cluster.clusterState = cluster.clusterState.toUpperCase();
      return cluster;
    })
    .filter((cluster) => {
      if (
        statusFilter &&
        cluster.clusterState.toUpperCase() !== statusFilter.toUpperCase()
      ) {
        return false;
      }
      if (
        search &&
        !cluster.name.toUpperCase().includes(search.toUpperCase())
      ) {
        return false;
      }
      // if the label is not rayllmbatchinference or rayjob, then it's a notebook
      // this relies on roblox flags
      if (
        (typeFilter == 2 && !clusterIsRayJob(cluster)) ||
        (typeFilter == 1 && clusterIsRayJob(cluster))
      ) {
        return false;
      }
      return true;
    })
    .map(transformCluster);

const transformCluster = (cluster: Cluster): ClusterRow => {
  return {
    name: cluster.name,
    clusterState: cluster.clusterState,
    createdAt: cluster.createdAt,
    links: {
      rayGrafanaDashboardLink: cluster.rayGrafanaDashboardLink,
      rayHeadDashboardLink: cluster.rayHeadDashboardLink,
      notebookLink:
        clusterIsNotebook(cluster) && !clusterIsRayJob(cluster)
          ? cluster.notebookLink
          : "",
    },
  };
};

export const clusterIsRayJob = (cluster: Cluster): boolean => {
  const jobType =
    cluster.clusterSpec?.headGroupSpec?.labels?.["mlp.rbx.com/component"];
  return jobType === "rayjob" || jobType === "rayllmbatchinference";
};

export const clusterIsNotebook = (cluster: Cluster): boolean => {
  const notebookType = cluster.annotations["mlp.rbx.com/notebook-type"];
  return notebookType === "jupyter" || notebookType === "vscode";
};
