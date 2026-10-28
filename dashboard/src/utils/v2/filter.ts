import { ClusterStatus } from "@/types/v2/raycluster";
import { JobStatus } from "@/types/v2/rayjob";
import { JobRow, ClusterRow } from "@/types/table";

export const filterJobs = (
  jobs: JobRow[],
  search: string,
  statusFilter: JobStatus | null,
  typeFilter: number,
): JobRow[] =>
  jobs
    .map((job) => {
      // Ensure jobStatus exists and has a default value
      if (!job.jobStatus.jobStatus) {
        job.jobStatus.jobStatus = "PENDING";
      }
      return job;
    })
    .filter((job: JobRow) => {
      // Status filter
      if (
        statusFilter &&
        job.jobStatus.jobStatus.toUpperCase() !== statusFilter.toUpperCase()
      ) {
        return false;
      }

      // Search filter
      if (search && !job.name.toUpperCase().includes(search.toUpperCase())) {
        return false;
      }

      // Type filter for Batch API
      if (typeFilter == 1) {
        // Check if it's a Batch API job by looking at labels
        const labels =
          job.clusterSpec?.headGroupSpec?.template?.metadata?.labels;
        if (
          !labels ||
          labels["mlp.rbx.com/component"] !== "rayllmbatchinference"
        ) {
          return false;
        }
      }

      return true;
    });

export const filterCluster = (
  clusters: ClusterRow[],
  search: string,
  statusFilter: ClusterStatus | null,
  typeFilter: number,
): ClusterRow[] => {
  return clusters.filter((cluster) => {
    if (statusFilter && cluster.clusterState !== statusFilter) {
      return false;
    }

    const searchUpper = search.toUpperCase();
    const nameMatch = cluster.name.toUpperCase().includes(searchUpper);
    const namespaceMatch = cluster.namespace
      .toUpperCase()
      .includes(searchUpper);

    if (!nameMatch && !namespaceMatch) {
      return false;
    }
    const isRayJob = clusterIsRayJob(cluster);

    if (typeFilter === 1 && isRayJob) {
      return false;
    }
    if (typeFilter === 2 && !isRayJob) {
      return false;
    }

    return true;
  });
};

export const clusterIsRayJob = (cluster: ClusterRow): boolean => {
  // Prefer official KubeRay label to determine whether a RayCluster originates from a RayJob
  const labels = cluster.labels || {};
  const originatedFrom = labels["ray.io/originated-from-crd"];
  if (originatedFrom?.toLowerCase() === "rayjob") {
    return true;
  }

  // Backward compatibility: support legacy custom labels if present
  const comp = labels["mlp.rbx.com/component"];
  return comp === "rayjob" || comp === "rayllmbatchinference";
};
