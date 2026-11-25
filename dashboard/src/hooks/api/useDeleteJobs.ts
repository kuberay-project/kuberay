import { mutate } from "swr";
import React from "react";
import { useSnackBar } from "@/components/SnackBarProvider";
import { useNamespace } from "@/components/NamespaceProvider";
import { config } from "@/utils/constants";
import { JobRow } from "@/types/table";
import { ALL_NAMESPACES } from "@/utils/config-defaults";

async function _deleteJob(namespace: string, jobName: string) {
  const baseUrl = `${config.rayApiUrl}/namespaces/${namespace}/rayjobs/`;
  const response = await fetch(`${baseUrl}${jobName}`, {
    method: "DELETE",
  });
  if (response.ok) {
    return await response.json();
  }
  console.log(response.status, response.statusText);
  const err = await response.json();
  throw new Error(err.message);
}

export const useDeleteJobs = () => {
  const [deleting, setDeleting] = React.useState(false);
  const namespace = useNamespace();
  const snackBar = useSnackBar();

  // Use the namespace of each row so that deletion works no matter which
  // namespace the dashboard is currently listing (including "all").
  const deleteJobs = async (jobs: readonly JobRow[]) => {
    setDeleting(true);
    // Delete the job and use "mutate" to force refresh the cache.
    // We don't want to use the result of deletion to populate cache, since the
    // response is empty.
    try {
      await mutate(
        `${namespace == ALL_NAMESPACES ? `` : `/namespaces/${namespace}`}/rayjobs`,
        Promise.all(jobs.map((job) => _deleteJob(job.namespace, job.name))),
        { populateCache: false },
      );
      snackBar.showSnackBar(
        `${jobs.length} job${jobs.length > 1 ? "s" : ""} deleted`,
        `Deleted ${jobs.map((job) => job.name).join(", ")}`,
        "success",
      );
    } catch (err) {
      snackBar.showSnackBar("Failed to delete jobs", "Error: " + err, "danger");
    }
    setDeleting(false);
  };

  return { deleting, deleteJobs };
};
